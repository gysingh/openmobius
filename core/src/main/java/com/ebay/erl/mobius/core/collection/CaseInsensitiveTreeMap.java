package com.ebay.erl.mobius.core.collection;

import java.util.TreeMap;

/**
 * Uses {@link String#CASE_INSENSITIVE_ORDER} 
 * as the key comparator.
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
public class CaseInsensitiveTreeMap extends TreeMap<String, String>
{	
	private static final long serialVersionUID = -821160304249434296L;

	public CaseInsensitiveTreeMap()
	{
		super(String.CASE_INSENSITIVE_ORDER);
	}
}
