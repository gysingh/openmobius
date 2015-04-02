package com.ebay.erl.mobius.core.criterion;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.ebay.erl.mobius.core.model.Tuple;

/**
 * Creates a composited {@link TupleCriterion} such as 
 * <code>A_Criteria and B_Criteria</code>.
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
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Jack Shen, Gyanit Singh, Neel Sundaresan
 */
public class LogicalExpression extends TupleCriterion
{		
	private static final long serialVersionUID = 3363448324144343780L;

	/**
	 * logical operator, AND or OR.
	 */
	public enum Operator implements Serializable
	{
		AND,
		OR
	}
	
	private TupleCriterion leftCriterion;
	
	private TupleCriterion rightCriterion;
	
	private Operator operator;
	
	/**
	 * Create a {@link TupleCriterion} equals to <code>leftCriterion</code> <code>op</code> <code>rightCriterion</code>,
	 * where <code>op</code> is either {@link Operator#AND} or {@link Operator#OR}. 
	 * 
	 * @param leftCriterion
	 * @param rightCriterion
	 * @param op
	 */
	public LogicalExpression(TupleCriterion leftCriterion, TupleCriterion rightCriterion, Operator op)
	{
		this.leftCriterion = leftCriterion;
		this.rightCriterion = rightCriterion;
		this.operator = op;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean evaluate(Tuple tuple, Configuration configuration)
	{
		switch(this.operator)
		{
			case AND:
				return this.leftCriterion.accept (tuple, configuration) && this.rightCriterion.accept (tuple, configuration);
			case OR:
				return this.leftCriterion.accept (tuple, configuration) || this.rightCriterion.accept (tuple, configuration);
			default:
				throw new IllegalArgumentException(this.operator+" is not supported");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String[] getInvolvedColumns()
	{
		List<String> allColumns = new LinkedList<String>();
		
		String[] c1 = this.leftCriterion.getInvolvedColumns ();
		for(String c:c1 )
		{
			allColumns.add (c);
		}
		
		String[] c2 = this.rightCriterion.getInvolvedColumns ();
		for(String c:c2 )
		{
			allColumns.add (c);
		}
		
		return allColumns.toArray (new String[0]);
	}	
}
