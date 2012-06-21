package com.ebay.erl.mobius.core.function;

import com.ebay.erl.mobius.core.function.base.SingleInputAggregateFunction;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.ResultWrapper;
import com.ebay.erl.mobius.core.model.Tuple;

/**
 * Counts the number of records of the given <code>inputColumn</code>
 * in a group.  Used only if the value of the given 
 * <code>inputColumn</code> is not null.
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
 */
public class Counts extends SingleInputAggregateFunction 
{	
	
	private static final long serialVersionUID = -8406996639392091020L;
	
	private long counts = 0L;

	/**
	 * Create an instance of {@link Counts} that
	 * counting the number of occurrence of 
	 * <code>inputColumn</code> in a group.
	 * <p>
	 * 
	 * If the value of <code>inputColumn</code>,
	 * the count won't increase.
	 */
	public Counts(Column inputColumn) 
	{
		super(inputColumn);
	}
	

	@SuppressWarnings("unchecked")
	@Override
	public void consume(Tuple tuple) 
	{
		Object value = tuple.get(this.inputColumnName);
		if( value!=null )
		{
			// value is not null, need to distinguish
			// either it's partial result generated
			// by a combiner or just regular record
			// value.
			byte type = Tuple.getType(value);
			if( type==Tuple.RESULT_WRAPPER_TYPE )
			{
				// it's partial result computed by
				// combiner
				counts += ((ResultWrapper<Long>)value).getCombinedResult();
				this.reporter.incrCounter("Mobius", "Partial Result Rows", 1);
			}
			else
			{
				// not partial result, just regular records,
				// simply increase the counter.
				this.reporter.incrCounter("Mobius", "Regular Result Rows", 1);
				counts++;
			}
		}
	}
	
	
	@Override
	protected Tuple getComputedResult()
	{		
		if( this.calledByCombiner() )
		{
			this.aggregateResult = new ResultWrapper<Long>(this.counts);
		}
		else
		{
			this.aggregateResult = Long.valueOf(this.counts);
		}
		return super.getComputedResult();
	}
	
	@Override
	public void reset()
	{
		super.reset();
		this.counts = 0L;
	}
	
	@Override
	public final boolean isCombinable()
	{
		return true;
	}
}
