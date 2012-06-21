package com.ebay.erl.mobius.core.model;

import java.io.Serializable;

import org.apache.hadoop.mapred.Reporter;

import com.ebay.erl.mobius.core.collection.BigTupleList;

/**
 * In a dataset building stage, this base class dynamically computes 
 * new columns for each row in the selected dataset.
 * <p>
 * 
 * For example, if a dataset contains the two columns: <code>UNIT_PRICE 
 * </code> and <code>QUANTITY</code>, users can add {@link ComputedColumns}
 * to compute <code>TOTAL_PRICE</code> as follows:
 * 
 * <pre>
 * <code>
 * new ComputedColumns("TOTAL_PRICE")
 * {
 * 	public void consume(Tuple newRow)
 * 	{
 * 		float price = newRow.getFloat("UNIT_PRICE");
 * 		int quantity = newRow.getInt("QUANTITY");
 * 		
 * 		Tuple result = new Tuple();
 * 		result.put("TOTAL_PRICE", price*quantity);
 * 
 * 		output(result);
 * 	}
 * }
 * </code>
 * </pre>
 * <p>
 * 
 * The {@link #consume(Tuple)} method produces more than one row
 * for each <code>newRow</code> if users invoke the {@link #output(Tuple)}
 * multiple times.
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
public abstract class ComputedColumns implements Serializable
{	
	private static final long serialVersionUID = 4386509620660443628L;
	
	private BigTupleList emptyResult;
	
	protected BigTupleList result;
	
	protected transient Reporter reporter;
	
	/**
	 * the schema of the output {@link Tuple} 
	 */
	protected String[] outputSchema;
	
	/**
	 * Create an instance of {@link ComputedColumns} which
	 * will add new column(s) to each row in a dataset.
	 * <p>
	 * The schema of the new column(s) are given by 
	 * <code>outputSchema</code>
	 */
	public ComputedColumns (String... outputSchema)
	{
		this.outputSchema = outputSchema;
	}
	
	/**
	 * Calculate the computed result based on the input
	 * row.  The schema of computed result must be same
	 * as the {@link #outputSchema}.
	 * <p>
	 * 
	 * When a computed result is generated, user can then
	 * use {@link #output(Tuple)} to emit the result.  Usually
	 * the {@link #output(Tuple)} is called once per row, but
	 * user has the freedom to call {@link #output(Tuple)} multiple
	 * times if the logic needs to produce multiple output records 
	 * per input records.
	 * <p>
	 * Example 1: one output record per input
	 * <pre>
	 * <code> 	
	 * public void consume(Tuple newRow)
	 * {
	 *   float usd = newRow.getFloat("USD");
	 *   float rate = newRow.getFloat("EXCHANGE_RATE");
	 *   
	 *   Tuple result = new Tuple();
	 *   result.put("TARGET_CURRENCY", usd*rate);
	 *   output(result);
	 * }
	 * </code>
	 * </pre>
	 * 
	 * Example 2: multiple output records per input.
	 * <pre>
	 * <code>
	 * // break down title into tokens, then later we can group by "TOKEN"
	 * // to calculate the frequency
	 * public void consume(Tuple newRow)
	 * {
	 *   String title = newRow.getString("title");
	 *   String[] tokens = title.toLowerCase().split("\\p{Space}+");
	 *   for ( String aToken:tokens )
	 *   {
	 *   	Tuple t = new Tuple();
	 *   	t.put("TOKEN", aToken);
	 *   	output(t);
	 *   }
	 * }
	 * </code>
	 * </pre>
	 */
	public abstract void consume(Tuple newRow);
	
	
	/**
	 * When user finished the computed result(s) in the {@link #consume(Tuple)},
	 * use this method to output the result.
	 * 
	 */
	protected final void output(Tuple t)
	{
		if( this.result==null )
		{
			this.result = new BigTupleList(this.reporter);
		}
		
		// only select the values in the output schema,
		// preventing user put some K-V that doesn't belong
		// to the specified output schema, which might overwrite
		// the original value of other tuple during the cross
		// product phase.
		Tuple r = new Tuple();
		for( String aColumn:this.outputSchema )
		{
			r.insert(aColumn, t.get(aColumn));
		}
		this.result.add(r);
	}
	
	/**
	 * Get the schema of the output of this {@link ComputedColumns}.
	 * 
	 */
	public final String[] getOutputSchema()
	{
		return this.outputSchema;
	}
	
	
	/**
	 * To be called by Mobius engine, the returned list contains
	 * zero to many {@link Tuple} which is computed and emitted 
	 * in {@link #consume(Tuple)}, each tuple has the same schema
	 * as the one specified in the constructor.
	 */
	public final BigTupleList getResult()
	{
		if( this.result==null || this.result.size()==0 )
		{
			if( emptyResult==null )
			{
				emptyResult = new BigTupleList(this.reporter);
				Tuple empty = new Tuple();
				for( String aColumn:this.getOutputSchema() )
				{
					empty.putNull(aColumn);
				}
				emptyResult.add(empty);
				emptyResult = BigTupleList.immutable(emptyResult);
			}
			return emptyResult;
		}			
		else
			return result;
	}
	
	/**
	 * To be called by Mobius engine when a new raw in a dataset
	 * come, this method will be called for every new raw for
	 * resetting previous result.
	 */
	public final void reset()
	{
		if( this.result!=null )
		{
			this.result.clear();
		}
	}
	
	
	public void setReporter(Reporter reporter){
		this.reporter = reporter;
	}
}
