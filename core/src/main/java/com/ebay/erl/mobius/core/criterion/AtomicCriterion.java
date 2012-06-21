package com.ebay.erl.mobius.core.criterion;

import org.apache.hadoop.conf.Configuration;

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
abstract class AtomicCriterion extends TupleCriterion
{	
	private static final long serialVersionUID = -6152332243434619856L;
	
	@SuppressWarnings("unused")
	private AtomicCriterion(){}
	
	protected String columnName;
	protected Object value;
	protected RelationalOperator op;
	
	public AtomicCriterion(String columnName, Object value, RelationalOperator op)
	{
		this.columnName = columnName;
		this.value = value;
		this.op = op;		
	}
	
	@Override
	public String toString()
	{
		return this.columnName+"\t"+this.op.toString ()+"\t"+this.value.toString ();
	}
	
	@Override
	public String[] getInvolvedColumns()
	{
		return new String[]{this.columnName};
	}
	
	@Override
	public boolean accept(Tuple tuple, Configuration configuration)
	{		
		// means the column is null, always return false
		if( (tuple.get (columnName))==null )
			return false;
		else
			return super.accept (tuple, configuration);		
	}
	
	@Override
	protected final boolean evaluate(Tuple tuple, Configuration configuration)
	{			
		switch(this.op)
		{
			case EQ:
				return this.eq (tuple);
			case NE:
				return this.ne (tuple);
			case GE:
				return this.ge (tuple);
			case GT:
				return this.gt (tuple);
			case LE:
				return this.le (tuple);
			case LT:
				return this.lt (tuple);
			case WITHIN:
				return this.within(tuple);
			case NOT_WITHIN:
				return this.not_within(tuple);
			case NOT_NULL:
				return this.not_null(tuple);
			default:
				throw new UnsupportedOperationException(this.op+" doesn't support.");
		}
	}
	
	protected boolean eq(Tuple tuple)
	{
		throw new UnsupportedOperationException();
	}
	protected boolean ne(Tuple tuple)
	{
		throw new UnsupportedOperationException();
	}
	protected boolean ge(Tuple tuple)
	{
		throw new UnsupportedOperationException();
	}
	protected boolean gt(Tuple tuple)
	{
		throw new UnsupportedOperationException();
	}
	protected boolean le(Tuple tuple)
	{
		throw new UnsupportedOperationException();
	}
	protected boolean lt(Tuple tuple)
	{
		throw new UnsupportedOperationException();
	}
	
	protected boolean within(Tuple tuple)
	{
		throw new UnsupportedOperationException();
	}
	
	protected boolean not_within(Tuple tuple)
	{
		throw new UnsupportedOperationException();
	}
	
	protected boolean not_null(Tuple tuple)
	{
		Object value = tuple.get(this.columnName);
		return value!=null;
	}
}
