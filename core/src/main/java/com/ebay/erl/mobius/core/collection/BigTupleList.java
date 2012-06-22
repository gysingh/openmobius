package com.ebay.erl.mobius.core.collection;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Reporter;

import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.util.JVMShutdownNotifier;

/**
 * Stores one to many {@link Tuple} elements.
 * <p>
 * 
 * By default, the tuples are stored in memory,
 * but if the memory is insufficient, the tuples 
 * are flushed into underlying file system.
 * <p>
 * 
 * When iterating the {@link Tuple} elements using 
 * {@link #iterator()}, the tuples are sorted based 
 * on the comparator if it is provided in the 
 * constructor, otherwise it would be in the insertion
 * order.
 * 
 * 
 * 
 * <p>
 * This product is licensed under the Apache License,  Version 2.0, 
 * available at http://www.apache.org/licenses/LICENSE-2.0.
 * 
 * This product contains portions derived from Apache hadoop which is 
 * licensed under the Apache License, Version 2.0, available at 
 * http://hadoop.apache.org.
 * 
 * � 2007 � 2012 eBay Inc., Evan Chiu, Woody Zhou, Neel Sundaresan
 */
public class BigTupleList implements NotificationListener, Observer, Iterable<Tuple>, Cloneable
{	
	private static final Log LOGGER	= LogFactory.getLog(BigTupleList.class);
	
	private static final long _MB				= 1024L*1024L;
	
	private static long _MAX_MEMORY	= -1;
	
	private static DecimalFormat _NBR_FORMAT;
	
	private static Runtime runtime;
	
	private static boolean _LOG_OUTPUT = false;
	
	private static long _GLOBE_ID = 0L;
	
	/**
	 * A synchronized key for increasing the ID.
	 */
	private static String KEY = "ID_KEY";
	
	
	/**
	 *  temporary output directory for the map-reduce job 
	 */
	private File workOutput;
	
	
	
	/**
	 * files that stored serialized {@link Tuple}s.
	 */
	List<File> buffer_on_disk;
	
	
	
	/**
	 * in-memory buffer
	 */
	List<Tuple> buffer_in_memory = Collections.synchronizedList(new ArrayList<Tuple>());
	
	
	/**
	 * total number of tuples within this list, including the ones
	 * on the disk, if any.
	 */
	private final AtomicLong totalTuples = new AtomicLong(0L);
	
	
	
	private boolean flushing = false;
	
	/**
	 * The first tuple in this list, it's used for estimating
	 * the total size of this BigTupleList.
	 */
	private Tuple firstTuple;
	
	
	boolean isMutable = true;
	
	
	Comparator<Tuple> comparator = null;	
	
	
	/**
	 * Hadoop status reporter
	 */
	Reporter reporter;
	
	/**
	 * ID for this big tuple list.
	 */
	long _ID = 0L;
	
	
	static
	{
		///////////////////////////////////
		// setup memory notification
		///////////////////////////////////
		List<MemoryPoolMXBean> memPools = ManagementFactory.getMemoryPoolMXBeans();
	    for (Iterator<MemoryPoolMXBean> i = memPools.iterator(); i.hasNext();) 
	    {
	    	MemoryPoolMXBean mp = i.next();
	    	MemoryUsage mu = mp.getUsage();
	    	long max = mu.getMax();
	    	long alert = (max * 50)/100; // alert when 50%
		    if (mp.isUsageThresholdSupported() ) 
		    {
		    	// Found the heap! Let's add a notifier		    	
		    	LOGGER.info("Setting a warning on usage: " + mp.getName() + " for: " + alert);
		    	mp.setUsageThreshold(alert);
		    	
		    }
		    if( mp.isCollectionUsageThresholdSupported() ){
		    	LOGGER.info("Setting a warning on collection usage: " + mp.getName() + " for: " + alert);
		    	mp.setCollectionUsageThreshold(alert);	
		    }
		    
	    }
	}
	
	
	/**
	 * A immutable zero size {@link BigTupleList}.
	 */
	public static final BigTupleList ZERO_SIZE_UNMODIFIABLE = new BigTupleList(null){
		
		@Override
		public void add(Tuple t)
		{
			throw new UnsupportedOperationException("This is a unmodifiable object.");
		}
	};
	
	
	/**
	 * Create an instance of {@link BigTupleList}
	 * with default comparator ({@link Tuple}).
	 * <p>
	 */
	public BigTupleList(Reporter reporter)		
	{
		this(null, reporter);
	}
	
	
	
	
	/**
	 * Create an instance of {@link BigTupleList} with
	 * specified <code>comparator</code>.
	 * <p>
	 * 
	 * {@link Tuple}s stored in this list is sorted by
	 * the <code>comparator</code>
	 */
	public BigTupleList(Comparator<Tuple> comparator, Reporter reporter)		
	{		
		if( comparator!=null )
			this.comparator = comparator;
		
		// register itself to the {@link MemoryMXBean} to receive
		// notification when the used memory exceed the threshold
		// (about to GC), the notification is handled in 
		// {@link #handleNotification(Notification, Object)}
		MemoryMXBean memBean 		= ManagementFactory.getMemoryMXBean();
	    NotificationEmitter ne		= (NotificationEmitter)memBean ;
	    //ne.addNotificationListener(this, null, null);
		
	    // setup local folder to store temporary files which contain
	    // tuples that cannot feet in memory.
	    
	    this.workOutput = new File("tmp");
	    if( !_LOG_OUTPUT )
		{
			LOGGER.info("working output is located in:"+this.workOutput.getAbsolutePath().toString());
			_LOG_OUTPUT = true;
		}
	    JVMShutdownNotifier.getInstance().addObserver(this);
	    
	    synchronized(KEY)
	    {
	    	_ID = _GLOBE_ID;
	    	_GLOBE_ID++;
	    }
	    
	    this.reporter = reporter;
	}
	
	public Tuple getFirst()
	{
		return this.firstTuple;
	}
	
	
	
	/**
	 * Check if this list is mutable or not.
	 */
	public boolean isMutable()
	{
		return this.isMutable;
	}
	
	
	
	/**
	 * Create a new instance of immutable {@link BigTupleList}
	 * which has identical content of the given <code>list</code>.
	 * <p>
	 * 
	 * This method returns a new instance, so <code>list</code>
	 * is still mutable.
	 */
	public static BigTupleList immutable(BigTupleList list)
	{
		BigTupleList clone	= list.clone();
		clone.isMutable		= false;
		return clone;	
	}
	
	
	
	/**
	 * Add a new Tuple into this {@link BigTupleList}.
	 * 
	 * @throws UnsupportedOperationException if this list is immutable.
	 */
	public void add(Tuple newTuple)
	{
		if( !this.isMutable )
		{
			throw new UnsupportedOperationException("This is an immutable instance, its content cannot be modified.");
		}
		
		
		if( this.buffer_in_memory.size()>=500000 )
			this.flushToDisk();
		this.buffer_in_memory.add(newTuple);
		if( this.firstTuple==null )
		{
			this.firstTuple = newTuple;
		}
		totalTuples.incrementAndGet();
	}
	
	
	/**
	 * Add all the tuples in <code>collection</code> into
	 * this list.
	 * 
	 * @throws UnsupportedOperationException if this list is immutable.
	 */
	public void addAll(Iterable<Tuple> collection)
	{
		Iterator<Tuple> it = collection.iterator();
		while(it.hasNext())
		{
			this.add(it.next());
		}
		
		if( it instanceof Closeable)
		{
			try {
				((Closeable)it).close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	
	
	/**
	 * Add all the tuples in <code>collection</code> into
	 * this list.
	 * 
	 * @throws UnsupportedOperationException if this list is immutable.
	 */
	public void addAll(Collection<Tuple> collection)
	{
		for(Tuple t:collection)
		{
			this.add(t);
		}
	}
	
	/**
	 * return the total number of {@link Tuple} stored in this
	 * {@link BigTupleList}
	 */
	public long size()
	{
		return this.totalTuples.get();
	}
	
	public long getEstimatedSizeInMemory()
	{
		if( this.size()==0 )
			return 0L;
		
		return this.size()*this.firstTuple.getEstimatedSizeInMemory();
	}
	
	/**
	 * Remove all {@link Tuple} in this {@link BigTupleList}
	 */
	public void clear()
	{
		LOGGER.debug(Thread.currentThread().getName()+" BID["+this._ID+"]"+"Clearing this tuple:"+this.totalTuples+" to be removed");
			
		// clear in memory records			
		buffer_in_memory.clear();
				
		// clear on disk records, if any.
		if( this.buffer_on_disk!=null )
		{
			for(File aTempFile:this.buffer_on_disk)
			{
				try
				{
					LOGGER.debug("Deleting "+aTempFile.getAbsolutePath());
					if( !aTempFile.delete() )
					{
						throw new IOException("Cannot delete file:"+aTempFile.getAbsolutePath());
					}
				}catch(Throwable e)
				{
					LOGGER.warn("Error when delete temp file:"+aTempFile.getAbsolutePath().toString(), e);
				}
			}
			this.buffer_on_disk.clear();
		}
		
		LOGGER.debug(Thread.currentThread().getName()+" BID["+this._ID+"] All tuples removed.");
		this.firstTuple = null;
		this.totalTuples.set(0L);
	}
	
	
		

	
	/**
	 * When the used memory exceed the threshold. this method will be called.
	 * <p>
	 * 
	 * If the number of tuples in this list is greater or equals to 1000,
	 * all the tuples will be saved into disk.
	 */
	@Override
	public void handleNotification(Notification notification, Object handback) 
	{	
		if( this == BigTupleList.ZERO_SIZE_UNMODIFIABLE)
			return;
		
		if( flushing )
			return;
		
		boolean performFlush = this.buffer_in_memory.size()>1000;
		
		if( performFlush )
		{
			LOGGER.warn(Thread.currentThread().toString()+" BID["+this._ID+"]"+"Almost OOM, total tuples:"+getNumberFormat().format(this.totalTuples)+", " +
				"in memory tuples:"+getNumberFormat().format(this.buffer_in_memory.size()));		
			this.flushToDisk();
		}
	}
	

	/**
	 * Listening to JVM shutdown signal.
	 * <p>
	 * 
	 * If JVM shutdown normally, this method will be called
	 * and {@link #clear()} will be called to remove all
	 * the stored tuples.
	 * 
	 * @see JVMShutdownNotifier
	 */
	@Override
	public void update(Observable o, Object arg) 
	{
		if( this == BigTupleList.ZERO_SIZE_UNMODIFIABLE )
			return;
		
		LOGGER.info("JVM is about to shuwdown, cleanning files on disk...");		
		this.clear();
		LOGGER.info("All temporal files are removed.");
	}

	
	
	private long availableMemory()
	{	
		long allocatedMemory	= getRuntime().totalMemory();
		long freeMemory			= getRuntime().freeMemory();
		return freeMemory + (getMaxMemory() - allocatedMemory);
	}
	
	
	
	private File newLocalFile()		
		throws IOException
	{	
		// creating a new file.
		File newFile = new File(this.workOutput, System.currentTimeMillis()+".tuples");
		try
		{
			while( newFile.exists() )
			{
				Thread.sleep(10);
				newFile = new File(this.workOutput, System.currentTimeMillis()+".tuples");
			}
		}
		catch (InterruptedException e) 
		{
			throw new RuntimeException("Cannot create new tempory file.", e);
		}
		
		// push the new file into buffer to remember it and return
		LOGGER.debug(Thread.currentThread().toString()+" BID["+this._ID+"]"+" new local file:"+newFile.getAbsolutePath());
		if( this.buffer_on_disk==null )
			this.buffer_on_disk = Collections.synchronizedList(new LinkedList<File>());
				
		this.buffer_on_disk.add(newFile);					
			
		return newFile;
	}
	
	
	/**
	 * Flush {@link Tuple}s in {@link #buffer_in_memory} into
	 * disk, and new local file will be created by {@link #newLocalFile()}
	 * and store the {@link File} reference in {@link #buffer_on_disk} for
	 * future reference.
	 */
	private void flushToDisk()
	{	
		this.flushing = true;
		File localFile;
		
		if( this.buffer_in_memory.size()==0 )
		{
			// no tuple in memory
			return;
		}
		long start = System.currentTimeMillis();	
		long availableMemory = this.availableMemory();
			
		String message = Thread.currentThread().toString()+" BID["+this._ID+"] "+
				"writing in-memory tuples ("+getNumberFormat().format(this.buffer_in_memory.size())+" entries) into disk, " +
				"available memory:"+availableMemory/_MB+"MB.";
				
		LOGGER.info(message);
		if( this.reporter!=null )
		{
			this.reporter.setStatus(message);
			this.reporter.progress();
		}
			
		try
		{
			// check if we still have enough local space to prevent 
			// full of disk exception.
			long freeDiskSpace = this.workOutput.getFreeSpace()/_MB;
			if( freeDiskSpace<300 )
			{
				// less than 300MB free space left, throw
				// exceptions
				throw new IOException("Not enough space left ("+freeDiskSpace+"MB remaining) on "+this.workOutput.getAbsolutePath()+".");
			}
				
			localFile = this.newLocalFile();
			DataOutputStream out = new DataOutputStream(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(localFile))));
					
			// write the tuple schema in the header
			String[] tupleSchema = this.buffer_in_memory.get(0).getSchema();
			out.writeInt(tupleSchema.length);
			if( tupleSchema.length==0 )
				throw new IllegalArgumentException("Tuple with empty schema!");
			for( String aColumn:tupleSchema )
			{
				out.writeUTF(aColumn);
			}
					
			// write number of tuple in this file
			out.writeLong(this.buffer_in_memory.size());
					
			if( this.comparator!=null )
			{
				// sort the Tuple in memory first
				Collections.sort(this.buffer_in_memory, this.comparator);
			}
					
			// write all the tuple in memory buffer
			long counts = 0L;
			for( Tuple aTuple:this.buffer_in_memory )
			{
				aTuple.write(out);
				counts++;
				if( counts%5000==0 && this.reporter!=null )// report every 5000 IO
					this.reporter.progress();
			}
			out.flush();
			out.close();
					
			// clear memory buffer
			this.buffer_in_memory.clear();			
				
				
			long end = System.currentTimeMillis();
				
			LOGGER.info(Thread.currentThread().toString()+" BID["+this._ID+"] "+
						"Write has completed, cost "+((end-start)/1000)+" seconds, " +
						"available memory:"+this.availableMemory()/_MB+"MB, " +
						"wrote to:"+localFile.getAbsolutePath()+"(size:"+localFile.getTotalSpace()/_MB+"MB) , " +
						"in memory tuples numbers:"+this.buffer_in_memory.size());
			
			this.flushing = false;
		}catch(IOException e)
		{
			throw new RuntimeException(e);
		}
	}
	
	
	/**
	 * Make a deep clone of this list and return a new
	 * instance.
	 * <p>
	 * 
	 * Tuples in memory will be cloned and the files store
	 * tuples in this list will be duplicated and stored
	 * in the returned list.
	 * <p>
	 * 
	 * Since it's a deep clone, make changes to this list
	 * won't change the returned clone.
	 * <p>
	 * 
	 * Mutable or not of the return clone is the same
	 * as this list.
	 */
	@Override
	public BigTupleList clone()
	{
		BigTupleList clone = new BigTupleList(this.comparator, this.reporter);
		
		clone.buffer_in_memory = new ArrayList<Tuple>();
		for( Tuple aTuple:this.buffer_in_memory )
		{
			clone.buffer_in_memory.add(aTuple.clone());
		}
			
		if( this.buffer_on_disk!=null )
		{
			clone.buffer_on_disk = new ArrayList<File>();
			try
			{	
				for( File file:this.buffer_on_disk )
				{										
					File copy = new File(file.toString()+".clone");
					FileUtils.copyFile(file, copy);
				}
			}catch(IOException e)
			{
				throw new RuntimeException("Cannot make file copy", e);
			}
		}
		
		clone.totalTuples.set(this.totalTuples.get());
		clone.isMutable		= this.isMutable;
		
		return clone;
	}
	
	
	
	/**
	 * Create an iterator for iterating the
	 * {@link Tuple} in this list.
	 * <p>
	 * 
	 * The {@link Tuple}s are sorted by a
	 * comparator, either the default one (
	 * {@link Tuple}) or the specified one
	 * in {@link #BigTupleList(Comparator)}
	 * <p>
	 * 
	 * It's it <b>very important</b> to invoke the
	 * {@link CloseableIterator#close()} after
	 * the iteration is done, whether the elements
	 * insides have been all iterated or not.
	 */
	@Override
	public CloseableIterator<Tuple> iterator() 
	{		
		// acquiring the lock and it is released only when the 
		// {@link CloseableIterator#close()} is called to prevent
		// any modification by other thread during the iteration 
		// time.
		
		LOGGER.debug(Thread.currentThread().toString()+" BID["+this._ID+"]"+" Start iterating.");		
		
		// calling availableMemory is expensive, make sure this BigTupleList is big
		// enough.
		if( this.buffer_in_memory.size()>50000 )
		{				
			// not enough memory, flush all data
			// into disk.
			LOGGER.debug("flush all tuples into disk.");
			this.flushToDisk();
		}
		else
		{
			if( this.comparator!=null )
			{
				// has enough memory, keep the <code>buffer_in_memory</code>
				// and sort it.
				LOGGER.debug("sort tuples in memory");
				Collections.sort(this.buffer_in_memory, this.comparator);
			}
		}
		
		// let the iterator close the lock
		LOGGER.debug("Returnning iterator");
		
		// number of tuples can be holded in memory.
		long bufferSize = 10000;
		
		LOGGER.debug("Number of tuples can be stored in memory when iterating:"+bufferSize);
		
		return new BigTupleListIterator((int)bufferSize, this);
	}
	
	
	private static synchronized Runtime getRuntime(){
		if( runtime==null )
			runtime = Runtime.getRuntime();
		return runtime;
	}
	
	private static synchronized DecimalFormat getNumberFormat(){
		if( _NBR_FORMAT==null )
			_NBR_FORMAT = new DecimalFormat("#,###");
		return _NBR_FORMAT;
	}
	
	private static long getMaxMemory(){
		if( _MAX_MEMORY==-1 )
			_MAX_MEMORY = getRuntime().maxMemory();
		return _MAX_MEMORY;
	}
}
