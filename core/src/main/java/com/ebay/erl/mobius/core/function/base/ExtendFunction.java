package com.ebay.erl.mobius.core.function.base;

import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;

/**
 * An extend function takes one row at a time in a group 
 * as its input, and produces one row as the output.  In other 
 * words, the output of <code>ExtendFunction</code> can be decided by 
 * just one input row does not require other records in a group. 
 * As a result, <code>ExtendFunction</code> cannot to access all the 
 * records in a group.
 * <p>
 * 
 * The output of an extended function can be one to many columns.  
 * For example, an extended function can be a general function that
 * performs A+B where the value of both A and B columns is numeric 
 * and the output of the function is the result of A+B.
 * <p>
 * 
 * The columns provided in the constructor are the inputs
 * to a extend function.
 * <p>
 * 
 * Implement the {@link #getResult(Tuple)} method to provide
 * the calculation logic.
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
 */
public abstract class ExtendFunction extends Projectable 
{	
	private static final long serialVersionUID = -1833625641383245897L;
	
	public ExtendFunction(Column[] inputs) 
	{
		super(inputs);
	}
	
	/**
	 * should be invoked by {@link Column} only
	 */
	protected ExtendFunction(){}
	
	/**
	 * Get the result of this function based on the <code>inputRow</code>,
	 * the schema of the returned {@link Tuple} shall be the same as
	 * {@link #getOutputSchema()}.
	 */
	public abstract Tuple getResult(Tuple inputRow);
	
	
	private transient Tuple noMatchRow = null;
	
	/**
	 * To be called by Mobius, on the case of no match
	 * in outer-join task.
	 */
	public final Tuple getNoMatchResult(Object nullReplacement)
	{
		if( this.noMatchRow==null )
		{
			Tuple temp = new Tuple();
			for( String aColumn:this.getOutputSchema() )
			{
				if( nullReplacement==null )
				{
					temp.putNull(aColumn);
				}
				else
				{
					temp.insert(aColumn, nullReplacement);
				}
			}
			this.noMatchRow = Tuple.immutable(temp);
		}
		return noMatchRow;
	}

}
