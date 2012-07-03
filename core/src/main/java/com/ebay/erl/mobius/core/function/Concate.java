package com.ebay.erl.mobius.core.function;

import org.apache.hadoop.mapred.TextOutputFormat;

import com.ebay.erl.mobius.core.function.base.SingleInputAggregateFunction;
import com.ebay.erl.mobius.core.model.Array;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.ResultWrapper;
import com.ebay.erl.mobius.core.model.Tuple;

/**
 * Concatenate all the values for the <code>inputColumn<code>
 * in a group into a single column, store the values in {@link Array}
 * and separated with <code>delimiter</code> if output as string.
 * <p>
 * 
 * The values within a group are stored in an {@link Array}, if
 * user wants to process the column in their original format, use
 * <code>build</code> instead of <code>save</code> since <code>save</code>
 * by default use <code>TextOuputFormat<code> and it will store the
 * values in string format.
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
@SuppressWarnings("deprecation")
public class Concate extends SingleInputAggregateFunction  
{	
	private static final long serialVersionUID = -4361665087023003610L;
	
	protected String delimiter = ";";// set default delimiter to semicolon
	
	protected transient Array array;

	/**
	 * Create a {@link Concate} function with
	 * semicolon as the default delimiter.  Note that,
	 * the delimiter is used only if the output format
	 * is {@link TextOutputFormat}.
	 */
	public Concate(Column inputColumn)
	{
		this(inputColumn, ";");
	}
	
	public Concate(Column inputColumn, String delimiter)
	{
		super(inputColumn);
		if( delimiter==null || delimiter.length()==0 )
			throw new IllegalArgumentException("Delimiter cannot be null nor empty.");
		
		this.delimiter = delimiter;
	}
	
	public Concate setDelimiter(String delimiter)
	{
		if( delimiter==null || delimiter.length()==0 )
			throw new IllegalArgumentException("Delimiter cannot be null nor empty.");
		
		this.delimiter = delimiter;
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void consume(Tuple tuple) 
	{
		if( this.array==null )
		{
			this.array = new Array(this.delimiter);
		}
		Object value = tuple.get(this.inputColumnName);
		
		byte type = Tuple.getType(value);
		if( type==Tuple.RESULT_WRAPPER_TYPE )
		{
			// result coming from Combiner
			ResultWrapper wrapper = (ResultWrapper)value;
			Array wrapped = (Array)wrapper.getCombinedResult();
			for( Object elem:wrapped )
			{
				array.add(elem);
			}
		}
		else
		{
			array.add(value);
		}
	}
	
	@Override
	public Tuple getComputedResult()
	{
		if( this.calledByCombiner() )
		{
			this.aggregateResult = new ResultWrapper<Array>(this.array);
		}
		else
		{
			this.aggregateResult = this.array;
		}
		return super.getComputedResult();
	}
	
	@Override
	public void reset()
	{
		super.reset();
		
		if( this.array!=null )
			this.array.clear();
	}

	
	@Override
	public boolean isCombinable()
	{
		return true;
	}
}
