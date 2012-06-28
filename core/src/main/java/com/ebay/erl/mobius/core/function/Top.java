package com.ebay.erl.mobius.core.function;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.collection.BigTupleList;
import com.ebay.erl.mobius.core.function.base.GroupFunction;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;
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
 * The output result is sorted in the reversed order of the
 * provided comparator.
 * 
 * <p>
 * This product is licensed under the Apache License,  Version 2.0, 
 * available at http://www.apache.org/licenses/LICENSE-2.0.
 * 
 * This product contains portions derived from Apache hadoop which is 
 * licensed under the Apache License, Version 2.0, available at 
 * http://hadoop.apache.org.
 * 
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Neel Sundaresan
 * 
 */
public class Top extends GroupFunction
{
	private static final long serialVersionUID = 9006860239763386840L;
	
	private transient Comparator<Tuple> comparator;
	
	private int topX;
	
	private String comparatorClassName;
	
	private transient PriorityQueue<Tuple> minHeap;
	
	
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
	 * Put the tuple into a min heap, once the heap
	 * size is greater than the specified {@link #topX},
	 * the smallest element (head of the heap) is poll 
	 * out from the heap.
	 */
	@Override
	public void consume(Tuple tuple) 
	{	
		if( this.minHeap==null )
		{
			this.minHeap = new PriorityQueue<Tuple>(this.topX, this.getComparator());
		}
		Tuple t = new Tuple();
		for( int i=0;i<this.getOutputSchema().length;i++ )		
		{
			String inName	= this.inputs[i].getInputColumnName();
			String outName	= this.getOutputSchema()[i];
			
			t.insert(outName, tuple.get(inName));
		}
		this.minHeap.add(tuple);
		
		while ( this.minHeap.size()>topX )
		{
			this.minHeap.poll();// remove the smallest
		}
	}
	
	
	@Override
	public BigTupleList getResult()
	{
		BigTupleList result = new BigTupleList(this.getComparator(), this.reporter);
		
		Tuple[] tuples = this.minHeap.toArray(new Tuple[this.minHeap.size()]);
		Arrays.sort(tuples, this.getComparator());
		
		for( int i=tuples.length-1;i>=0;i-- ){
			result.add(tuples[i]);
		}
		return result;
	}
	
	@Override
	public void reset()
	{
		super.reset();
		if ( this.minHeap!=null )
			this.minHeap.clear();
	}
	
	
	
	
	
	@SuppressWarnings("unchecked")
	private Comparator<Tuple> getComparator()
	{
		if(this.comparator==null && comparatorClassName!=null)
		{
			try
			{
				this.comparator = (Comparator<Tuple>)Util.getClass(this.comparatorClassName).newInstance();				
			}catch(Throwable e)
			{
				throw new RuntimeException("Cannot create instance of comparator:"+this.comparatorClassName, e);
			}
		}
		else if ( this.comparator==null && comparatorClassName==null ){
			this.comparator = new Tuple();
		}
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
