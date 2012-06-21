package com.ebay.erl.mobius.core.criterion;

import java.util.Collections;
import java.util.List;

import com.ebay.erl.mobius.core.model.Tuple;

/**
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
class NumberCriterion extends AtomicCriterion 
{
	private static final long serialVersionUID = 1729537105362395270L;	
	
	public NumberCriterion(String columnName, Double value, RelationalOperator op) 
	{
		super(columnName, value, op);
	}
	
	@SuppressWarnings("unchecked")
	public NumberCriterion(String columnName, List<Double> value, RelationalOperator op) 
	{
		super(columnName, value, op);
		Collections.sort((List<Double>)this.value);
	}
	
	protected boolean eq(Tuple tuple)
	{
		return tuple.getDouble(this.columnName).compareTo((Double)this.value)==0;
	}
	protected boolean ne(Tuple tuple)
	{
		return tuple.getDouble(this.columnName).compareTo((Double)this.value)!=0;
	}
	protected boolean ge(Tuple tuple)
	{
		return tuple.getDouble(this.columnName).compareTo((Double)this.value)>=0;
	}
	protected boolean gt(Tuple tuple)
	{
		return tuple.getDouble(this.columnName).compareTo((Double)this.value)>0;
	}
	protected boolean le(Tuple tuple)
	{
		return tuple.getDouble(this.columnName).compareTo((Double)this.value)<=0;
	}
	protected boolean lt(Tuple tuple)
	{
		return tuple.getDouble(this.columnName).compareTo((Double)this.value)<0;
	}
	
	@SuppressWarnings("unchecked")
	protected boolean within(Tuple tuple)
	{
		return Collections.binarySearch((List<Double>)this.value, tuple.getDouble(this.columnName))>=0;
	}
	
	@SuppressWarnings("unchecked")
	protected boolean not_within(Tuple tuple)
	{
		return Collections.binarySearch((List<Double>)this.value, tuple.getDouble(this.columnName))<0;
	}
}
