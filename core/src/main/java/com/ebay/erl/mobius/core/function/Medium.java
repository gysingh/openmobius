package com.ebay.erl.mobius.core.function;

import java.util.Comparator;

import com.ebay.erl.mobius.core.collection.BigTupleList;
import com.ebay.erl.mobius.core.collection.CloseableIterator;
import com.ebay.erl.mobius.core.function.base.SingleInputAggregateFunction;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.util.Util;

/**
 * Gets the medium value of the <code>inputColumn</code> in
 * a group.  The ordering is natural ordering by default, user
 * can override the ordering by providing a comparator.
 * 
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
public class Medium extends SingleInputAggregateFunction 
{
	private static final long serialVersionUID = 4163999711314409439L;
	
	private transient Comparator<Tuple> comparator;
	
	private String comparatorClassName;
	
	private transient BigTupleList temp;
	
	
	/**
	 * Create an instance of {@link Medium} operation to
	 * get the medium value of the given 
	 * <code>inputColumn</code> within a group.
	 * <p>
	 * 
	 * The comparing is natural ordering.
	 */
	public Medium(Column inputColumn) 
	{
		this(inputColumn, null);
	}
	
	
	/**
	 * Create an instance of {@link Medium} operation to
	 * get the medium value of the given 
	 * <code>inputColumn</code> within a group.
	 * <p>
	 * 
	 * The comparing is done by user specified <code>comparator</code>.
	 */
	public Medium(Column inputColumn, Class<? extends Comparator<Tuple>> comparator)
	{
		super(inputColumn);
		if( comparator!=null )
			this.comparatorClassName = comparator.getCanonicalName();
	}
	
	/**
	 * override from parent to have the returned {@link BigTupleList}
	 * use user defined comparator, if any.
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected BigTupleList newBigTupleList()
	{
		if( this.comparator==null && comparatorClassName!=null )
		{
			// user has specified a customized comparator,
			// initialize it only once.
			try
			{
				this.comparator = (Comparator<Tuple>)Util.getClass(this.comparatorClassName).newInstance();				
			}catch(Throwable e)
			{
				throw new RuntimeException("Cannot create instance of comparator:"+this.comparatorClassName, e);
			}
		}
		
		// the <code>this.comparator</code> can still be
		// empty here if user doesn't specify a customized
		// comparator, if that's the case, BigTupleList
		// will use the default comparator.
		return new BigTupleList(this.comparator, this.reporter);
	}
	

	@Override
	public void consume(Tuple tuple) 
	{
		// the idea is extract the value from
		// the record (tuple), push it into
		// a BigTupleList, let the BigTupleList
		// perform the sorting, and extract the
		// medium value in the getComputedResult()
		// method.
		Object newValue = tuple.get(this.inputColumnName);		
		Tuple t = new Tuple();
		t.insert(this.inputColumnName, newValue);
		if( this.temp==null)
			this.temp = this.newBigTupleList();
		
		this.temp.add(t);
	}
	
	@Override
	public Tuple getComputedResult()
	{
		CloseableIterator<Tuple> it = this.temp.iterator();		
		long mediumIdx	= this.temp.size()/2;
		long counts		= 0L;
		Tuple result	= null;
		while( it.hasNext() )
		{
			Tuple t = it.next();
			if( counts<mediumIdx )
			{
				counts++;
			}
			else
			{
				result = t;
				break;
			}
		}
		it.close();
		
		this.aggregateResult = result.get(this.inputColumnName);
		return super.getComputedResult();
	}
	
	@Override
	public void reset()
	{
		super.reset();
		if( this.temp==null)
			this.temp = this.newBigTupleList();
		this.temp.clear();		
	}
}
