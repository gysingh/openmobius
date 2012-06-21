package com.ebay.erl.mobius.core.sort;

import java.io.Serializable;


/**
 * Specifies the sorting columns and the
 * sorting ordering in a total sort job.
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
public class Sorter implements Serializable
{
	
	private static final long serialVersionUID = -5414337925998337052L;

	/**
	 * Ordering type.
	 */
	public enum Ordering
	{
		/**
		 * ascending order.
		 */
		ASC,
		
		/**
		 * descending order.
		 */
		DESC,
	}
	
	private String sortColumn;
	private Ordering ordering;
	private boolean forceSortNumerically;
	
	
	/**
	 * Create a sorter for a sort job.
	 * <p>
	 * 
	 * Sorter is used to specify the ordering or the
	 * records in a dataset.
	 * 
	 * @param columnName column to be sorted
	 * @param ordering  ascending or descending order.
	 * @param forceSortNumerically <code>true</code> to force compare 
	 * the value of the given <code>columnName</code> numerically, 
	 * <code>false</code> to use the native comparing of the original
	 * value type.
	 */
	public Sorter(String columnName, Ordering ordering, boolean forceSortNumerically)
	{
		this.sortColumn			= columnName;
		this.ordering			= ordering;
		this.forceSortNumerically	= forceSortNumerically;
	}
	
	
	/**
	 * Create a sorter for a sort job.
	 * <p>
	 * 
	 * Sorter is used to specify the ordering or the
	 * records in a dataset.
	 * 
	 * @param columnName column to be sorted
	 * @param ordering  ascending or descending by the
	 * nature ordering of the <code>sortColumn</code>.
	 * 
	 */
	public Sorter(String sortColumn, Ordering ordering)
	{
		this(sortColumn, ordering, false);
	}
	
	
	public boolean forceSortNumerically()
	{
		return this.forceSortNumerically;
	}
	
	public Ordering getOrdering()
	{
		return this.ordering;
	}
	
	public String getColumn()
	{
		return this.sortColumn;
	}
	
	
	@Override
	public String toString()
	{
		return "Sorter("+this.sortColumn+", "+this.ordering.toString()+")";
	}
}
