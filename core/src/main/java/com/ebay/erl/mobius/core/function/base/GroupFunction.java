package com.ebay.erl.mobius.core.function.base;

import com.ebay.erl.mobius.core.collection.BigTupleList;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;

/**
 * A group function takes all the records in a group first, and 
 * then based on the inputs, produces X number of rows as the 
 * output, where X can be zero to many.
 * <p>
 * 
 * When a new record comes, the consume(Tuple) method is called. 
 * After all the records in a group are iterated through, Mobius 
 * engine retrieves the output of the group function via the 
 * {@link #getResult()} method.
 * <p>
 * 
 * The implementation of a group function might not require all 
 * the records in a group to calculate the final result.  In this 
 * case, Mobius still feeds all the records to the consume method, 
 * the implementer can choose to ignore any records that are not
 * needed.
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
public abstract class GroupFunction extends Projectable
{	
	private static final long serialVersionUID = -4239742588506235301L;

	private static final BigTupleList EMPTY_RESULT = BigTupleList.ZERO_SIZE_UNMODIFIABLE;
	
	
	/**
	 * The container to hold the result been pushed
	 * by the {@link #output(Tuple)} method within
	 * a group.
	 */
	protected transient BigTupleList rowsToBeOutputted;
	
	
	
	/**
	 * Create a {@link GroupFunction} which takes the <code>inputs</code>
	 * to compute some result.  The schema of the result will be, by default, 
	 * <code>this.getClass().getSimpleName()+"_"+aColumn.getOutputName()</code>,
	 * for each <code>inputs</code>.
	 * <p>
	 * 
	 * The number of output column doesn't have to be the same as the number 
	 * of input column, user can use {@link #setOutputSchema(String...)} to set 
	 * the real output schema.
	 */
	public GroupFunction(Column... inputs)
	{
		super(inputs);
	}
	
	
	protected BigTupleList getRowsToBeOutputted()
	{
		if( this.rowsToBeOutputted==null )
			this.rowsToBeOutputted = new BigTupleList(this.reporter);
		return this.rowsToBeOutputted;
	}
	
	
	
	/**
	 * consume a value within a group, to be implemented
	 * by sub-class.
	 */
	public abstract void consume(Tuple tuple);
	
	
	
	/**
	 * Empty previous result (<code>rowsToBeOutputted</code>), reset is 
	 * called when the values within a group have been all iterated.
	 * <p>
	 * 
	 *  It is important to call super.reset() when override this method 
	 *  in a sub-class, fail to do so, will result in wrong result.
	 */
	public void reset()
	{
		if( this.rowsToBeOutputted!=null )
			this.rowsToBeOutputted.clear();
	}
	
	
	
	/**
	 * To be called by the sub-class when a computed result can
	 * be populated.
	 * <p>
	 * 
	 * The number of output rows (within a group) of this function 
	 * is equals to the number of times this method is called.
	 */
	protected void output(Tuple tuple)
	{
		if( this.rowsToBeOutputted==null )
		{
			this.rowsToBeOutputted = new BigTupleList(this.reporter);
		}
		
		this.rowsToBeOutputted.add(tuple);
	}
	
	
	/**
	 * result for the no match case on outer-join
	 * job.
	 */
	private transient BigTupleList noMatchResult = null;
	
	public final BigTupleList getNoMatchResult(Object nullReplacement)
	{
		if( noMatchResult==null )
		{
			noMatchResult = new BigTupleList(this.reporter);
			
			Tuple nullRow = new Tuple();
			for( String aColumn:this.getOutputSchema() )
			{
				if( nullReplacement==null )
				{
					nullRow.putNull(aColumn);
				}
				else
				{
					nullRow.insert(aColumn, nullReplacement);
				}
			}
			noMatchResult.add(Tuple.immutable(nullRow));
			this.noMatchResult = BigTupleList.immutable(this.noMatchResult);
		}
		return this.noMatchResult;
	}
	
	
	
	/**
	 * Get the computed result.
	 * <p>
	 * 
	 * The computed result will be cross-product with
	 * results from other functions ( if any).
	 */
	public BigTupleList getResult()
	{
		if( this.rowsToBeOutputted==null )
			return EMPTY_RESULT;
		else
			return this.rowsToBeOutputted;
	}	
}
