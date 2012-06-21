package com.ebay.erl.mobius.core.function;

import java.math.BigDecimal;

import com.ebay.erl.mobius.core.function.base.SingleInputAggregateFunction;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;

/**
 * Computes the average value of the given <code>inputColumn</code>
 * specified in {@link #Avg(Column)}.
 * <p>
 * 
 * {@link Avg} supports column in the following type, numerical type 
 * ({@link Tuple#isNumericalType(byte)} return true), date type 
 * ({@link Tuple#isDateType(byte)} return true), string, or writable.
 * <p>
 * 
 * In the case of string or writable type, this class try
 * try to parse the number into double from its string 
 * representation. If it cannot be converted into such a number, an 
 * exception is thrown. 
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
public class Avg extends SingleInputAggregateFunction 
{
	private static final long serialVersionUID = 4064217644716020759L;
	
	protected static final byte _UNSET = -1;

	/**
	 * the type of the value of the input column
	 */
	protected byte valueType = _UNSET;
	
	
	/**
	 * sum of the value of the selected column
	 * within a group.
	 */
	protected BigDecimal sum;
	
	
	/**
	 * number of records in a group.
	 */
	protected long total = 0L;
	
	
	/**
	 * the scale of the final output, default is 0.
	 */
	protected final int scale;
	
	
	/**
	 * the rounding mode of the final output,
	 * default if {@link BigDecimal#ROUND_HALF_UP}.
	 */
	protected final int roundingMode;
	
	
	/**
	 * Create an {@link Avg} instance with scale set to 0 and
	 * rounding mode to {@link BigDecimal#ROUND_HALF_UP}
	 */
	public Avg(Column inputColumn) 
	{
		this(inputColumn, 0, BigDecimal.ROUND_HALF_UP);
	}
	
	/**
	 * Create an {@link Avg} instance with the given scale and 
	 * rounding mode.
	 *  
	 * @param scale scale of the returned result
	 * @param roundingMode rounding mode (one of the BigDecimal#ROUND_XXX) of the returned result
	 */
	public Avg(Column inputColumn, int scale, int roundingMode) 
	{
		super(inputColumn);
		this.scale = scale;
		this.roundingMode = roundingMode;
		this.reset();
	}

	@Override
	public void consume(Tuple tuple) 
	{
		Object newValue = tuple.get(this.inputColumnName);
		
		if( newValue==null )
			return;
		
		total++;
		
		if( valueType!=_UNSET )
		{
			this.accumulateSum(tuple);
		}	
		else
		{	
			this.valueType			= Tuple.getType(newValue);			
			this.accumulateSum(tuple);
		}
	}
	
	
	private void accumulateSum(Tuple tuple)
	{
		if( Tuple.isNumericalType(this.valueType) )
		{
			this.sum = sum.add( BigDecimal.valueOf(tuple.getDouble(this.inputColumnName)) );
		}
		else if( Tuple.isDateType(this.valueType) )
		{
			java.util.Date date = (java.util.Date)tuple.get(this.inputColumnName);
			this.sum = sum.add( BigDecimal.valueOf(date.getTime()) );
		}
		else if( this.valueType==Tuple.STRING_TYPE )
		{
			// assume the value of this column can be parsed into
			// number.
			try
			{
				this.sum = sum.add( new BigDecimal(tuple.getString(this.inputColumnName)) );
			}
			catch(NumberFormatException e)
			{
				throw new NumberFormatException("the value of column["+this.inputColumnName+"] is " +
						"["+tuple.getString(this.inputColumnName)+"] and cannot be parsed into number.");
			}
		}
		else if( this.valueType==Tuple.WRITABLE_TYPE )
		{
			// call the toString() method, and assume the value can
			// be parsed into number, the Tuple#getString will call
			// the toString() method of the value object.
			try
			{
				this.sum = sum.add( new BigDecimal(tuple.getString(this.inputColumnName)) );
			}
			catch(NumberFormatException e)
			{
				throw new NumberFormatException("the string representation of column["+this.inputColumnName+"] is " +
						"["+tuple.getString(this.inputColumnName)+"] and cannot be parsed into number.");
			}
		}
		else
		{
			throw new UnsupportedOperationException(" The type of column ["+this.inputColumnName+"] is "+Tuple.getTypeString(valueType) +
				", cannot perform average calculation on this type.");
		}
	}
	
	@Override
	protected Tuple getComputedResult()
	{
		if( Tuple.isDateType(this.valueType) )
		{
			BigDecimal result = this.sum.divide(BigDecimal.valueOf(this.total));
			java.util.Date date = null;
			switch(this.valueType)
			{
				case Tuple.DATE_TYPE:
					date = new java.sql.Date(result.longValue());
					break;
				case Tuple.TIME_TYPE:
					date = new java.sql.Time(result.longValue());
					break;
				case Tuple.TIMESTAMP_TYPE:
					date = new java.sql.Timestamp(result.longValue());
					break;
				default:
					throw new IllegalArgumentException(Tuple.getTypeString(valueType)+" is not date type.");
			}
			this.aggregateResult = date;
		}
		else
		{
			BigDecimal result = this.sum.divide(BigDecimal.valueOf(this.total));
			result.setScale(this.scale, this.roundingMode);
			
			this.aggregateResult = result;
		}
		
		return super.getComputedResult();
	}
	
	@Override
	public void reset() 
	{
		super.reset();
		this.total	= 0L;
		this.sum	= new BigDecimal(0D);
		this.sum.setScale(this.scale, this.roundingMode);
	}
}