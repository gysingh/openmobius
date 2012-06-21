package com.ebay.erl.mobius.core.function.base;

import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;

/**
 * 
 * A single input aggregate function is a special type of 
 * aggregate function.  It takes single column from a 
 * dataset as its input and consumes all the records in a 
 * group to generate a single row as its output.  
 * <p>
 * 
 * This class enforces the limit of one column to the 
 * constructor.
 * <p>
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
 * See {@link #getComputedResult()} about the output of {@link SingleInputAggregateFunction}.
 *
 */
public abstract class SingleInputAggregateFunction extends AggregateFunction
{
	
	private static final long serialVersionUID = 3601837074053219122L;
	
	/**
	 * shortcut for the name of the input column.
	 */
	protected String inputColumnName;
	
	
	
	public SingleInputAggregateFunction(Column inputColumn)
	{
		super(new Column[]{inputColumn});
		
		this.inputColumnName = this.inputs[0].getInputColumnName();
	}
	
	
	/**
	 * Return the computed result in a {@link Tuple}.
	 * <p>
	 * 
	 * By default, the returned {@link Tuple} contains only
	 * one column, the name of the column is the first
	 * element in the {@link #getOutputSchema()}.
	 * <p>
	 * 
	 * Override this method if there is a need to output a
	 * {@link Tuple} with more than one column.
	 */
	@Override
	protected Tuple getComputedResult()
	{
		Tuple record = new Tuple();
		record.insert(this.getOutputSchema()[0], this.aggregateResult);
		return record;
	}
}
