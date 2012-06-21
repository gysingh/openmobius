package com.ebay.erl.mobius.core.function;

import com.ebay.erl.mobius.core.collection.BigTupleList;
import com.ebay.erl.mobius.core.collection.CloseableIterator;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;


/**
 * Computes the number of unique values for the given 
 * <code>inputColumns</code> in a group.
 * <p>
 * 
 * Uniqueness is measured within the values from
 * the specified <code>inputColumns</code> in 
 * {@link #UniqueCounts(Column...)} in a group.
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
public class UniqueCounts extends Unique  
{	
	private static final long serialVersionUID = -9054645032498004503L;	
	
	/**
	 * Create an instance of {@link UniqueCounts} to calculate
	 * number of unique rows within a group.
	 * <p>
	 * 
	 * Uniqueness is measured only within the values
	 * from <code>inputColumns<code>.
	 */
	public UniqueCounts(Column... inputColumns)
	{
		super(inputColumns);
	}

	
	/**
	 * Override the implementation from {@link Unique},
	 * only output single row per group.  The row contains
	 * only one column which represents the number of
	 * unique rows in a group.
	 */
	@Override
	public BigTupleList getResult()
	{
		long uniqueCounts = 0L;
		
		Tuple previous = null;
		
		CloseableIterator<Tuple> it = this.temp.iterator();
		while( it.hasNext() )
		{
			Tuple current = it.next();
			
			if( previous==null || previous.compareTo(current)!=0 )
			{	
				// a different tuple comes, add the result
				uniqueCounts++;
			}			
			previous = current;
		}
		it.close();
		
		Tuple result = new Tuple();
		result.put(this.getOutputSchema()[0], uniqueCounts);
		this.output(result);
		
		return super.getResult();
	}
}
