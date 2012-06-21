package com.ebay.erl.mobius.core.criterion;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.core.model.TupleColumnComparator;


/**
 * To create a {@link TupleCriterion} instance that evaluate
 * the values relationship between two given columns in a row. 
 * 
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
class ColumnsCriterion extends TupleCriterion 
{	
	private static final long serialVersionUID = -5467194445713678674L;
	
	/**
	 * one column from a dataset.
	 */
	protected String column1;
	
	/**
	 * another column from the same dataset.
	 */
	protected String column2;
	
	/**
	 * comparison operator for the specified
	 * two columns. 
	 */
	protected RelationalOperator operator;

	protected transient TupleColumnComparator comparator;
	
	/**
	 * Create a criterion that check the values from <code>column1</code> and
	 * </code>column2</code> follows the given <code>operator</code> or not.
	 * <p>
	 * Only the following types of {@link RelationalOperator} is supported:
	 * {@linkplain RelationalOperator#EQ}, {@linkplain RelationalOperator#NE}, 
	 * {@linkplain RelationalOperator#GE}, {@linkplain RelationalOperator#GT},
	 * {@linkplain RelationalOperator#LE} or {@linkplain RelationalOperator#LT}
	 * 
	 * @throws IllegalArgumentException if the specified <code>operator</code> is not
	 * in the list above.
	 */
	
	protected final String[] involvedColumns;
	
	/**
	 * 
	 * @param column1 name of a column in a dataset.
	 * @param column2 name of another column in the same dataset.
	 * @param operator comparison operator.
	 */
	public ColumnsCriterion(String column1, String column2, RelationalOperator operator)
	{
		this.column1	= column1;
		this.column2	= column2;
		this.operator	= operator;
		
		// make sure user doesn't use operator other than EQ, NE, GE
		// GT, LE, LT.
		switch(this.operator)
		{				
			case EQ:
				break;
			case NE:
				break;
			case GE:
				break;				
			case GT:
				break;				
			case LE:
				break;				
			case LT:
				break;
			default:
				throw new IllegalArgumentException(this.operator.toString()+" is not supported, only "+
						RelationalOperator.EQ+", "+
						RelationalOperator.NE+", "+
						RelationalOperator.GE+", "+
						RelationalOperator.GT+", "+
						RelationalOperator.LE+", "+
						RelationalOperator.LT+" is supported");
		}
		
		this.involvedColumns = new String[]{this.column1, this.column2};
	}
	
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean evaluate(Tuple tuple, Configuration configuration) 
	{
		if( this.comparator==null )
		{
			this.comparator = new TupleColumnComparator();
		}
		
		Object v1 = tuple.get(column1);
		Object v2 = tuple.get(column2);
		
		try 
		{
			int result = comparator.compare(v1, v2, configuration);
			switch(operator)
			{				
				case EQ:
					return result==0;
				case NE:
					return result!=0;
				case GE:
					return result>=0;// means v1 >= v2				
				case GT:
					return result>0;// means v1 > v2				
				case LE:
					return result<=0;// means v1 <= v2				
				case LT:
					return result<0;// means v1 < v2
				case WITHIN:
					throw new IllegalArgumentException(operator.toString()+" is not supported in "+ColumnsCriterion.class.getSimpleName());				
				case NOT_WITHIN:
					throw new IllegalArgumentException(operator.toString()+" is not supported in "+ColumnsCriterion.class.getSimpleName());
				case NOT_NULL:
					throw new IllegalArgumentException(operator.toString()+" is not supported in "+ColumnsCriterion.class.getSimpleName());
				default:
					throw new IllegalArgumentException(operator.toString()+" is not a validate enumeration.");
			}
		}
		catch (IOException e) 
		{
			throw new RuntimeException(e);
		}
	}

	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String[] getInvolvedColumns() 
	{
		return this.involvedColumns;
	}

}
