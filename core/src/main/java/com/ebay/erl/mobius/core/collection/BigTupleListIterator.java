package com.ebay.erl.mobius.core.collection;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ebay.erl.mobius.core.model.Tuple;

/**
 * <p>
 * This product is licensed under the Apache License,  Version 2.0, 
 * available at http://www.apache.org/licenses/LICENSE-2.0.
 * 
 * This product contains portions derived from Apache hadoop which is 
 * licensed under the Apache License, Version 2.0, available at 
 * http://hadoop.apache.org.
 * 
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Jack Shen, Gyanit Singh, Neel Sundaresan
 *
 */
class BigTupleListIterator implements CloseableIterator<Tuple>
{
	private static final Log LOGGER = LogFactory.getLog(BigTupleListIterator.class);
	
	
	/**
	 * A number of {@link Tuple}s to ask underline sources 
	 * (in-memory or on-disk) to provide in order to get
	 * top X tuple.
	 * <p>
	 * 
	 * This number also control how many number of tuples
	 * can be return directly from memory without performing
	 * {@link #fillSortedBuffer()}, which involve sorting and 
	 * may involve disk I/O.
	 * <p>
	 * 
	 * Tuples in the underline sources are already sorted
	 * using the comparator in the {@link BigTupleList}
	 */
	private int bufferSize = 100;
	
	
	/**
	 * A counter to remember how many tuples have
	 * been return by the {@link #next()} method, 
	 * if this number >= <code>bufferSize</code>,
	 * then {@link #fillSortedBuffer()} will be called
	 * in order to get the next top X tuples.
	 */
	private long currentReadTuples = 0;
	
	
	/**
	 * underline sources store the tuples.
	 */
	private ArrayList<TupleSource> tupleSources;	
	
	
	
	
	/**
	 * A list that is used to sort the tuples
	 * in memory.
	 * <p>
	 * 
	 * Ask up to <code>bufferSize</code> tuples from
	 * each of the underline sources, and place them 
	 * in this array to sort, in order to get top
	 * <code>bufferSize</code> tuples.
	 * <p>
	 * 
	 * This list contains up to 
	 * <code>bufferSize</code> x {@link #tupleSources}.size()
	 * tuples.
	 */
	private LinkedList<TupleToBuffer> sortingBuffer;
	
	
	/**
	 * a boolean flag to indicate if all the underline
	 * sources have no more tuples, i.e., all the tuples
	 * have been read.
	 */
	private boolean allBuffersDrained = false;
	
	private TupleToBufferComparator comparator;
	
	private BigTupleList bigList;
	
	private long totalRecords;
	
	public BigTupleListIterator(int sort_buffer, BigTupleList bigList)
	{
		
		this.bigList		= bigList;
		this.totalRecords 	= this.bigList.size();		
		this.bufferSize		= sort_buffer;
		
		int sizePerBlock	= 1/* 1 for buffer in memory*/;
		if( bigList.buffer_on_disk!=null )
		{
			sizePerBlock += bigList.buffer_on_disk.size();
		}
		
		this.tupleSources 	= new ArrayList<TupleSource>();
		
		// index to indicate the position of newly 
		// created {@link TupleSource} in the {@link #tupleSources} 
		int queueIdx = 0;
		
		// add on disk tuples first as they were inserted firstly.
		if( bigList.buffer_on_disk!=null )
		{
			// add on disk tuples buffer
			for(File aFile:bigList.buffer_on_disk)
			{
				this.tupleSources.add(new DiskSource(sizePerBlock, queueIdx++, aFile));
			}
		}
		// add in memory tuples buffer
		this.tupleSources.add( new MemorySource(sizePerBlock, queueIdx++, bigList.buffer_in_memory) );
		
		if( bigList.comparator!=null )
			this.comparator = new TupleToBufferComparator(bigList.comparator);
		this.fillSortedBuffer();
	}
	
	
	@Override
	public boolean hasNext() 
	{	
		if( this.currentReadTuples==this.bufferSize )
		{
			// has returned top X tuples within the
			// {@link #sortingBuffer}, need to reload
			// the buffer in order to get the next top
			// X.				
			this.fillSortedBuffer();
		}
		
		if( this.sortingBuffer.size()>0 )
		{
			return true;
		}
		else
		{
			if( allBuffersDrained )
			{
				this.bigList.isMutable = true;
				LOGGER.debug(Thread.currentThread().toString()+ " BID["+this.bigList._ID+"] all iterated.");
				return false;
			}
			else
			{
				// the {@link allBuffersDrained} has not
				// been set, ask to fill sorting buffer, 
				// and check its size again. 
				this.fillSortedBuffer();
				if( this.sortingBuffer.size()==0 )
				{
					// sorting buffer still empty, 
					// no more elements
					this.allBuffersDrained = true;
					
					LOGGER.debug(Thread.currentThread().toString()+ " BID["+this.bigList._ID+"] all iterated.");
					return false;
				}
				else
				{
					return true;
				}
			}	
		}
	}


	@Override
	public Tuple next() 
	{
		if( this.hasNext() )
		{	
			this.checkConcurrentModification();
			
			// pop the first one
			TupleToBuffer elem = this.sortingBuffer.removeFirst();
			
			// get the corresponding source
			this.tupleSources.get(elem.belongedQueueIdx).remove();
			this.currentReadTuples++;
			
			if( this.currentReadTuples%1000==0 && this.bigList.reporter!=null ){
				this.bigList.reporter.setStatus("Iterated "+this.currentReadTuples+" tuples.");
			}
			
			return elem.tuple;
		}
		else
		{
			throw new NoSuchElementException("No more element available.");
		}
	}


	/**
	 * always throws UnsupportedOperationException
	 */
	@Override
	public void remove() 
	{
		throw new UnsupportedOperationException();
	}
	
	
	
	@Override
	public void close()
	{
		this.sortingBuffer.clear();
		for( TupleSource aSource:this.tupleSources )
		{
			aSource.close();
		}
		this.tupleSources.clear();
		LOGGER.debug("Close iterator.");
	}
	
	
	private void checkConcurrentModification()
	{
		if( this.bigList.size()!=this.totalRecords )
		{
			throw new ConcurrentModificationException();
		}
	}
	
	
	/**
	 * fulfill the sorting buffer from the underline buffer
	 * and sort it.
	 */
	private void fillSortedBuffer()
	{	
		if( this.sortingBuffer!=null )
		{	
			this.sortingBuffer.clear();
		}
		else
		{			
			this.sortingBuffer = new LinkedList<TupleToBuffer>();
		}
		this.currentReadTuples = 0;
		
		for( TupleSource aBuffer:tupleSources )
		{
			this.sortingBuffer.addAll(aBuffer.fill());
		}
		
		if( this.sortingBuffer.size()>0 && this.comparator!=null )
		{
			Collections.sort(this.sortingBuffer, this.comparator);
		}
	}
	
	
	private static class TupleToBufferComparator implements Comparator<TupleToBuffer>
	{
		private Comparator<Tuple> comparator;
		
		public TupleToBufferComparator(Comparator<Tuple> comparator)
		{
			this.comparator = comparator;
		}

		@Override
		public int compare(TupleToBuffer o1, TupleToBuffer o2) 
		{
			return this.comparator.compare(o1.tuple, o2.tuple);
		}
	}
	
	private static class TupleToBuffer
	{
		Tuple tuple;
		int belongedQueueIdx;
		
		@Override
		public String toString()
		{
			return this.belongedQueueIdx+"\t"+tuple.toString();
		}
	}
	
	
	/**
	 * represents a type of source of tuples, can be
	 * from memory or from disk file.
	 */
	private abstract static class TupleSource implements Closeable
	{
		protected final int bufferSize;
		
		protected final int assignedQueueIdx;
		
		protected LinkedList<TupleToBuffer> buffer;
		
		public TupleSource(int bufferSize, int assignedQueueIndex)
		{
			this.bufferSize			= bufferSize;
			this.assignedQueueIdx 	= assignedQueueIndex;
			this.buffer				= new LinkedList<TupleToBuffer>();
		}
		
		public abstract List<TupleToBuffer> fill();
		
		public final void remove()
		{
			this.buffer.removeFirst();
		}
		
		@Override
		public void close()
		{
			this.buffer.clear();
		}
	}
	
	
	private static class DiskSource extends TupleSource
	{
		/**
		 * the file contains serialized tuples
		 */
		private File source;
		
		/**
		 * total number of tuples in the file.
		 */
		private long totalTuples;
		
		/**
		 * number of tuples has been read so far.
		 */
		private long currentReadTuples = 0;
		
		/**
		 * schema of the underline tuple.
		 */
		private String[] schema;
		
		/**
		 * reader responsible to deserialize tuples
		 * from the <code>on_disk</code>
		 */
		private DataInputStream reader;
		
		
		
		public DiskSource(int bufferSize, int assignedQueueIndex, File on_disk)
		{
			super(bufferSize, assignedQueueIndex);
			
			try 
			{
				reader = new DataInputStream(new BufferedInputStream(new GZIPInputStream(new FileInputStream(on_disk))));
				
				// load the schema of the underline tuples.
				int schemaLength = reader.readInt();
				this.schema = new String[schemaLength];
				for( int i=0;i<schemaLength;i++)
				{
					schema[i] = reader.readUTF();
				}
				
				this.totalTuples = reader.readLong();
			}
			catch (IOException e) 
			{
				throw new RuntimeException("Cannot read underline file:"+on_disk.getAbsolutePath(), e);
			}
			this.buffer = new LinkedList<TupleToBuffer>();
		}

		@Override
		public List<TupleToBuffer> fill() 
		{
			while ( this.buffer.size()<this.bufferSize && this.currentReadTuples<totalTuples )
			{					
				Tuple t = new Tuple();
				try 
				{
					t.readFields(this.reader);
					t.setSchema(this.schema);	
				} 
				catch (IOException e) 
				{
					throw new RuntimeException("Cannot deserialize tuples from underline file:"+source.toString(), e);
				}				
				
				TupleToBuffer elem		= new TupleToBuffer();
				elem.tuple				= t;
				elem.belongedQueueIdx	= this.assignedQueueIdx;
				this.buffer.add(elem);
				this.currentReadTuples++;
			}
			
			if( this.currentReadTuples>=totalTuples)
			{
				try{this.reader.close();}catch(IOException e){}
			}
			return buffer;
		}
		
		@Override
		public void close()
		{
			super.close();
			try{this.reader.close();}catch(IOException e){}
		}
	}
	
	
	private static class MemorySource extends TupleSource
	{
		/**
		 * the tuples source.
		 */
		private List<Tuple> source;
		
		private int currentIdx = 0;
		
		public MemorySource(int bufferSize, int sn, List<Tuple> in_memory)
		{
			super(bufferSize, sn);
			this.source	= in_memory;
			this.buffer		= new LinkedList<TupleToBuffer>();
		}

		@Override
		public List<TupleToBuffer> fill() 
		{		
			while ( this.buffer.size()<this.bufferSize && this.currentIdx<this.source.size() )
			{
				TupleToBuffer elem 	= new TupleToBuffer();
				elem.tuple			= this.source.get(this.currentIdx);
				elem.belongedQueueIdx		= this.assignedQueueIdx;
				this.buffer.add(elem);
				this.currentIdx++;
			}
			return buffer;
		}
	}		
}