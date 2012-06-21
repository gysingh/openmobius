package com.ebay.erl.mobius.core.function.base;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ebay.erl.mobius.core.collection.BigTupleList;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;


/**
 * An aggregate function is a specific type of group 
 * function that takes all records in a group, and 
 * outputs only one row.  The output columns of an 
 * aggregate function can still be one to many columns.
 * <p>
 * 
 * To implement an aggregate function, override the methods 
 * {@link #consume(Tuple)} and {@link #getComputedResult()} 
 * as well as implement the logic in these two methods.  The 
 * consume method is called by Mobius each time there 
 * is a new  record in a group.  Users can store some partially 
 * computed results during this stage.  The {@link #getComputedResult()} 
 * method returns one tuple; users should implement the logic of 
 * basing the final result on the partial results here.  
 * <p>
 * 
 * Note that, the schema of the returned tuple is the 
 * same as the schema Mobius retrieved from {@link #getOutputSchema()}.
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
public abstract class AggregateFunction extends GroupFunction 
{	
	
	@SuppressWarnings("unused")
	private static final Log LOGGER = LogFactory.getLog(AggregateFunction.class);
	
	private static final long serialVersionUID = -853177997378037294L;
	
	/**
	 * the computed result.
	 * <p>
	 * This object will be set to null every time
	 * when a new group starts, done by the {{@link #reset()}
	 * method.
	 */
	protected Object aggregateResult;
	
	
	
	/**
	 * Constructor, can take 1 to more columns as
	 * it's input.
	 */
	public AggregateFunction(Column[] inputs)
	{
		super(inputs);
	}	
	
	
	/**
	 * Get the computed result for this group.
	 * <p>
	 * 
	 * The returned {@link Tuple} shall contains the
	 * same schema as the return value of 
	 * {@link #getOutputSchema()}.
	 * 
	 */
	protected abstract Tuple getComputedResult(); 

	
	
	/**
	 * {@inheritDoc}
	 * <p>
	 * 
	 * Force this method can be called only once per group, make
	 * this method to <code>final</code> to  prevent subclass 
	 * violate the contract.
	 * <p>
	 * 
	 * @throws IllegalStateException if this method is called more than once within
	 * a group.
	 */
	@Override
	protected final void output(Tuple tuple)
	{
		if( this.rowsToBeOutputted==null )
		{
			this.rowsToBeOutputted = this.newBigTupleList();
		}		
		
		if( this.rowsToBeOutputted.size()==0 && tuple!=null )
		{
			this.rowsToBeOutputted.add(tuple);
		}
		else
		{
			throw new IllegalStateException(this.getClass().getSimpleName()+" can only emit one row per group.");
		}
	}
	
	
	protected BigTupleList newBigTupleList()
	{
		return new BigTupleList(this.reporter);
	}
	
	/**
	 * {@inheritDoc}
	 * <p>
	 * 
	 * Use the {@link Tuple} returned by the {@link #getComputedResult()}
	 * as the only output and set this method to <code>final</code> to 
	 * prevent subclass violate the contract.
	 */
	@Override
	public final BigTupleList getResult()
	{
		this.output(this.getComputedResult());
		return super.getResult();
	}
	
	
	
	/*
	 * {@inheritDoc}
	 * <p>
	 * 
	 * Also set the <code>aggregateResult</code> to <code>null</code>.
	 */
	@Override
	public void reset()
	{
		super.reset();
		this.aggregateResult = null;
	}
}
