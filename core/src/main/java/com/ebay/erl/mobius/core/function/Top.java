package com.ebay.erl.mobius.core.function;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.collection.BigTupleList;
import com.ebay.erl.mobius.core.function.base.GroupFunction;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.core.model.TupleColumnComparator;
import com.ebay.erl.mobius.core.sort.Sorter;
import com.ebay.erl.mobius.util.Util;


/**
 * Returns up to <code>topX</code> {@link Tuple} 
 * in a group.
 * <p>
 * 
 * Natural ordering is used by default. Users can override
 * the ordering by providing a customized comparator in
 * {@link #Top(Dataset, String[], int, Class)}.
 * <p>
 * 
 * Top X is defined as: tuples are sorted by natural ordering, customized
 * comparator ({@link #Top(Dataset, String[], int, Class)}), or
 * user provided sorters ( {@link #Top(Dataset, String[], Sorter[], int)}).
 * When tuples are sorted, it pick the first X elements in the sorted
 * result.<br>
 * For example, a tuple list with one single column and their values are
 * [2, 7, -1, 3, 10, 2, 4], then if people chose to use natural ordering,
 * and want top 3 element, and the top 3 elements would be {-1, 2, 2}.  If
 * user chose to use <code>Sorter(Ordering.DESC)</code>, then top 3 are
 * {4, 7, 10}.
 * <p>
 * 
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
public class Top extends GroupFunction
{
	private static final long serialVersionUID = 9006860239763386840L;
	
	private transient Comparator<Tuple> comparator;
	
	private int topX;
	
	private String comparatorClassName;
	
	private transient PriorityQueue<Tuple> maxHeap;
	
	private Sorter[] sorters;
	
	
	/**
	 * Create a {@link Top} operation that emit up to
	 * <code>topX</code> rows from the given dataset 
	 * <code>ds</code>.
	 * <p>
	 * 
	 * The ordering is natural ordering by comparing the
	 * value of <code>inputColumns</code>, in sequence.
	 */
	public Top(Dataset ds, String[] inputColumns, int topX)
	{
		this(ds, inputColumns, topX, null);
	}
	
	
	/**
	 * Create a {@link Top} operation that emit up to
	 * <code>topX</code> rows from the given dataset 
	 * <code>ds</code>.
	 * <p>
	 * 
	 * The ordering is defined by the specified <code>comparator</code>,
	 * note that, the comparator can only access columns in
	 * <code>inputColumns</code>.
	 */
	public Top(Dataset ds, String[] inputColumns, int topX, Class<? extends Comparator<Tuple>> comparator)
	{
		super(getColumns(ds, inputColumns));
		this.comparatorClassName = comparator.getCanonicalName();
		this.topX = topX;
	}
	
	/**
	 * Create a {@link Top} operation that emit up to
	 * <code>topX</code> rows from the given dataset 
	 * <code>ds</code>.
	 * <p>
	 * 
	 * The ordering is defined by the specified <code>sorters</code>,
	 * note that, the <code>sorters</code> can only access columns in
	 * <code>inputColumns</code>.
	 */
	public Top(Dataset ds, String[] inputColumns, Sorter[] sorters, int topX)
	{
		super(getColumns(ds, inputColumns));
		
		if( sorters==null || sorters.length==0 )
		{
			throw new IllegalArgumentException("sorters cannot null nor empty.");
		}
		
		// make sure the "sorters" only use the columns in the
		// <code>inputColumns</code>
		for(Sorter aSorter:sorters )
		{
			boolean withinSchema = false;
			for( String aColumn: inputColumns)
			{
				if( aSorter.getColumn().equalsIgnoreCase(aColumn) )
				{
					withinSchema = true;
					break;
				}
			}
			
			if( !withinSchema )
			{
				throw new IllegalArgumentException("A sorter uses column["+aSorter.getColumn()+"] " +
						"that is not in the input columns:"+Arrays.toString(inputColumns));
			}
		}
		
		this.topX = topX;
		this.sorters = sorters;
	}

	/**
	 * Put the tuple into a min heap, once the heap
	 * size is greater than the specified {@link #topX},
	 * the smallest element (head of the heap) is poll 
	 * out from the heap.
	 */
	@Override
	public void consume(Tuple tuple) 
	{	
		if( this.maxHeap==null )
		{
			this.maxHeap = new PriorityQueue<Tuple>(this.topX, this.getComparator());
		}
		
		this.maxHeap.add(tuple);
		
		while ( this.maxHeap.size()>topX )
		{
			this.maxHeap.poll();// remove the smallest
		}
	}
	
	
	@Override
	public BigTupleList getResult()
	{
		BigTupleList result = new BigTupleList(this.reporter);
		
		Tuple[] tuples = this.maxHeap.toArray(new Tuple[this.maxHeap.size()]);
		Arrays.sort(tuples, this.getComparator());
		
		for( int i=tuples.length-1;i>=0;i-- )
		{
			Tuple currentTuple = tuples[i];
			Tuple t = new Tuple();
			for( int j=0;j<this.getOutputSchema().length;j++ )		
			{
				String inName	= this.inputs[j].getInputColumnName();
				String outName	= this.getOutputSchema()[j];
				
				t.insert(outName, currentTuple.get(inName));
			}
			result.add(t);
		}
		return result;
	}
	
	@Override
	public void reset()
	{
		super.reset();
		if ( this.maxHeap!=null )
			this.maxHeap.clear();
	}
	
	
	
	
	
	@SuppressWarnings("unchecked")
	private Comparator<Tuple> getComparator()
	{
		if( this.comparator!=null )
			return this.comparator;
		
		Comparator<Tuple> comp = null;
		
		if(this.comparator==null && comparatorClassName!=null)
		{
			try
			{
				comp = (Comparator<Tuple>)Util.getClass(this.comparatorClassName).newInstance();				
			}catch(Throwable e)
			{
				throw new RuntimeException("Cannot create instance of comparator:"+this.comparatorClassName, e);
			}
		}
		else if ( this.comparator==null && comparatorClassName==null )
		{
			if( this.sorters==null)
			{
				// user doesn't specified sorters nor customized comparator,
				// use the default comparator (Tuple).
				comp = new Tuple();
			}
			else
			{
				// user has specified sorters, create a comparator that
				// use sorters to compare.
				comp = new Comparator<Tuple>(){
				
					TupleColumnComparator comparator = new TupleColumnComparator();
					
					@Override
					public int compare(Tuple t1, Tuple t2) 
					{
						int result = comparator.compareKey(t1, t2, sorters, getConf());
						return result;
					}
				};
			}
		}
		
		final Comparator<Tuple> finalComp = comp;
		
		this.comparator = new Comparator(){
			@Override
			public int compare(Object o1, Object o2) 
			{
				// reverse order as use max heap to get top x
				return - finalComp.compare((Tuple)o1, (Tuple)o2);
			}
		};
		
		return this.comparator;
	}
	
	
	private static Column[] getColumns(Dataset ds, String[] columns)
	{
		Column[] result = new Column[columns.length];
		
		for( int i=0;i<columns.length;i++ )
		{
			result[i] = new Column(ds, columns[i]);
		}
		return result;
	}
	
}
