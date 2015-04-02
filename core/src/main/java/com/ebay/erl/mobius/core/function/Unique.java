package com.ebay.erl.mobius.core.function;

import com.ebay.erl.mobius.core.collection.BigTupleList;
import com.ebay.erl.mobius.core.collection.CloseableIterator;
import com.ebay.erl.mobius.core.function.base.GroupFunction;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;

/**
 * Returns the unique rows in a group.
 * <p>
 * 
 * Uniqueness is measured within the values from
 * the specified <code>columns</code> (in {@link #Unique(Column...)})
 * in a group.
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
public class Unique extends GroupFunction
{
	private static final long serialVersionUID = -709140700573771345L;
	
	/**
	 * temporal list to store values in a group.
	 */
	protected BigTupleList temp;
	
	/**
	 * Create an instance of {@link Unique} to emit
	 * unique rows within a group.
	 * <p>
	 * 
	 * Uniqueness is measured only within the values
	 * from <code>columns<code>.
	 */
	public Unique(Column...columns)
	{
		super(columns);
	}
	

	@Override
	public void consume(Tuple tuple) 
	{
		Tuple t = new Tuple();
		for( int i=0;i<this.inputs.length;i++ )
		{
			String outName	= this.outputSchema[i];
			String inName	= this.inputs[i].getInputColumnName();
			t.insert(outName, tuple.get(inName));
		}
		
		if( this.temp==null )
			this.temp = new BigTupleList(new Tuple(), this.reporter);
		
		this.temp.add(t);
	}
	
	@Override
	public BigTupleList getResult()
	{
		// the <code>temp</code> is sorted, so
		// we just iterate it over and check if 
		// the previous tuple is different than
		// the current tuple or not.
		
		Tuple previous = null;
		
		CloseableIterator<Tuple> it = this.temp.iterator();
		while( it.hasNext() )
		{
			Tuple current = it.next();
			
			if( previous==null || previous.compareTo(current)!=0 )
			{	
				// a different tuple comes, add the result
				this.output(current);
			}			
			previous = current;
		}
		it.close();
		
		return super.getResult();
	}
	
	@Override
	public void reset()
	{
		super.reset();
		if( this.temp==null )
			this.temp = new BigTupleList(this.reporter);
		this.temp.clear();
	}

}
