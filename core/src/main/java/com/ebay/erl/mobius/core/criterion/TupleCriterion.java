package com.ebay.erl.mobius.core.criterion;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.ebay.erl.mobius.core.criterion.LogicalExpression.Operator;
import com.ebay.erl.mobius.core.model.Tuple;

/**
 * Evaluates a {@link com.ebay.erl.mobius.core.model.Tuple}.
 * <p>
 * 
 * If the tuple is accepted after calling
 * {@link TupleCriterion#accept(Tuple, Configuration)}, then 
 * the tuple is populated.
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
 * @see TupleRestrictions
 */
public abstract class TupleCriterion implements Configurable, Serializable
{	
	private static final long serialVersionUID = -6167137205672538682L;
	
	private transient Configuration conf;
	
	/**
	 * Test if the <code>tuple</code> meet this {@linkplain TupleCriterion} or not
	 * 
	 * @param tuple a tuple in a dataset to be tested
	 * @param configuration
	 * @return <code>true</code> if the <code>tuple</code> pass this {@linkplain TupleCriterion}, <code>false</code> otherwise.
	 */
	public boolean accept(Tuple tuple, Configuration configuration)
	{
		boolean result = this.evaluate (tuple, configuration);
		return result;
	}
	
	
	/**
	 * return a new {@link TupleCriterion} that represents
	 * <code>this AND another</code>
	 * <p>
	 * 
	 * The <code>another</code> and <code>this</code> 
	 * {@link TupleCriterion} doesn't change.
	 */
	public TupleCriterion and(TupleCriterion another)
	{
		LogicalExpression and = new LogicalExpression(this, another, Operator.AND);
		return and;
	}
	
	
	/**
	 * return a new {@link TupleCriterion} that represents
	 * <code>this OR another<code>.
	 * <p>
	 * 
	 * The <code>another</code> and <code>this</code> {@link TupleCriterion}
	 * doesn't change.
	 */
	public TupleCriterion or(TupleCriterion another)
	{
		LogicalExpression or = new LogicalExpression(this, another, Operator.OR);
		return or;
	}
	
	
	/**
	 * Return a new instance of {@linkplain TupleCriterion} that
	 * is the complement of this one.
	 * <p>
	 * The original {@linkplain TupleCriterion} will not be changed.
	 * 
	 *  
	 * @return complement of this {@linkplain TupleCriterion}
	 */
	public final TupleCriterion not()
	{
		final TupleCriterion original = this;
		
		TupleCriterion notThis = new TupleCriterion()
		{			
			private static final long serialVersionUID = -1761924012973264041L;

			@Override
			protected boolean evaluate(Tuple tuple, Configuration configuration)
			{
				boolean result = original.evaluate (tuple, configuration);				
				return !result;
			}

			@Override
			public String[] getInvolvedColumns()
			{	
				return original.getInvolvedColumns ();
			}
			
		};
		
		return notThis;
	} 
	
	
	/**
	 * Sub class shall override this method to verify if the <code>tuple</code>
	 * meet the the criteria or not.
	 *  
	 * @param tuple a tuple to be test
	 * @param configuration Hadoop configuration
	 * @return true if the <code>tuple</code> meets this criteria, false
	 * otherwise.
	 */
	protected abstract boolean evaluate(Tuple tuple, Configuration configuration);
	
	
	/**
	 * return an array of column names that are required
	 * by this criterion. 
	 */
	public abstract String[] getInvolvedColumns();
	
	
	@Override
	public void setConf(Configuration conf)
	{
		this.conf = conf;
	}

	@Override
	public Configuration getConf()
	{
		return this.conf;
	}
	
	
	/**
	 * validate if the given <code>criteria</code> only use
	 * the columns in the <code>allAvailableColumns</code>
	 * 
	 * @param allAvailableColumns
	 * @param criteria
	 * @throws IllegalArgumentException if the <code>criteria</code> use some column(s) that
	 * doesn't exist in the <code>allAvailableColumns</code>
	 */
	public static void validate(Set<String> allAvailableColumns, TupleCriterion criteria)
		throws IllegalArgumentException
	{
		// transform the <code>allAvailableColumns</code> into case-insensitive
		// set
		TreeSet<String> set = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
		set.addAll(allAvailableColumns);
		
		for(String aColumn:criteria.getInvolvedColumns() )
		{
			if( !set.contains(aColumn) )
			{
				throw new IllegalArgumentException("["+aColumn+"] is required by the specified criteria, but doesn't exist in "+set);
			} 
		}
	}
	
}
