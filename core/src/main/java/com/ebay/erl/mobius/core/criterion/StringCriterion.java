package com.ebay.erl.mobius.core.criterion;

import java.util.Collections;
import java.util.List;

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
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Neel Sundaresan
 *
 */
class StringCriterion extends AtomicCriterion
{

	private static final long serialVersionUID = -7349548721815928009L;

	StringCriterion(String columnName, String value, RelationalOperator op)
	{
		super (columnName, value, op);
	}
	
	StringCriterion(String columnName, List<String> value, RelationalOperator op)
	{
		super (columnName, value, op);
		Collections.sort(value);
	}

	@Override
	protected boolean eq(Tuple currentRow)
	{
		return this.getValue(currentRow).equals (this.value);
	}
	@Override
	protected boolean ne(Tuple currentRow)
	{
		return !this.getValue(currentRow).equals (this.value);
	}

	@Override
	protected boolean ge(Tuple currentRow)
	{
		return this.getValue(currentRow).compareTo ((String)this.value)>=0;
	}

	@Override
	protected boolean gt(Tuple currentRow)
	{
		return this.getValue(currentRow).compareTo ((String)this.value)>0;
	}

	@Override
	protected boolean le(Tuple currentRow)
	{
		return this.getValue(currentRow).compareTo ((String)this.value)<=0;
	}

	@Override
	protected boolean lt(Tuple currentRow)
	{
		return this.getValue(currentRow).compareTo ((String)this.value)<0;
	}
	
	@SuppressWarnings("unchecked")
	protected boolean within(Tuple tuple)
	{
		return Collections.binarySearch((List<String>)this.value, this.getValue(tuple))>=0;
	}
	
	@SuppressWarnings("unchecked")
	protected boolean not_within(Tuple tuple)
	{
		return Collections.binarySearch((List<String>)this.value, this.getValue(tuple))<0;
	}

	private String getValue(Tuple currentRow)
	{
		return currentRow.getString(this.columnName);
	}
}
