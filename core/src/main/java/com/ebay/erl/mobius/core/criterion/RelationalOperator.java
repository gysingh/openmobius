package com.ebay.erl.mobius.core.criterion;


/**
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
public enum RelationalOperator 
{
	/**
	 * equals to (==)
	 */
	EQ,
	
	/**
	 * not equals to (!=)
	 */
	NE,
	
	/**
	 * greater than or equals to (>=)
	 */
	GE,
	
	/**
	 * greater than (>)
	 */
	GT,
	
	/**
	 * less than or equals to (<=)
	 */
	LE,
	
	/**
	 * less than (<)
	 */
	LT,
	
	WITHIN,
	
	NOT_WITHIN,
	
	NOT_NULL,
}
