package com.ebay.erl.mobius.core.function;

import java.math.BigDecimal;

import com.ebay.erl.mobius.core.function.base.SingleInputAggregateFunction;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;

public class Sum extends SingleInputAggregateFunction 
{
		
	private static final long serialVersionUID = 8043394371071057906L;
	
	
	/**
	 * Create an instance of {@link Max} operation to
	 * get the maximum value of the given 
	 * <code>inputColumn</code> within a group.
	 * <p>
	 * 
	 * The comparing is natural ordering.
	 */
	public Sum(Column inputColumn) 
	{
		super(inputColumn);
	}

	
	@Override
	public void consume(Tuple tuple) 
	{
		Object newValue = tuple.get(this.inputColumnName);
		
		byte type = Tuple.getType(newValue);
		
		if( Tuple.isNumericalType(type) )
		{
			this.add(((Number)newValue).doubleValue());
		}
		else if( type==Tuple.STRING_TYPE )
		{
			// try to convert it to double
			try
			{
				Double.parseDouble((String)newValue);
			}
			catch(NumberFormatException e)
			{
				throw new NumberFormatException(newValue.toString()+" cannot be converted into double.");
			}
		}
		else
		{
			throw new IllegalArgumentException(Tuple.getTypeString(type)+" is not numerical type for column:"+this.inputColumnName+" with value:"+newValue);
		}
	}
	
	private void add(double value)
	{
		if( this.aggregateResult==null )
		{
			this.aggregateResult = new BigDecimal(0D);
		}
		this.aggregateResult = ((BigDecimal)this.aggregateResult).add(BigDecimal.valueOf(value));
	}
	
	@Override
	public void reset()
	{
		super.reset();
		this.aggregateResult = new BigDecimal(0D);
	}
	
	@Override
	public final boolean isCombinable()
	{
		return true;
	}
}