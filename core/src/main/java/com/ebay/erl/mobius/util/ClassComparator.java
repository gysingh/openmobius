package com.ebay.erl.mobius.util;

import java.util.Comparator;

/**
 * Comparator for {@link Class}, put sub-classes
 * in front of super-classes.
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
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Neel Sundaresan
 */
public class ClassComparator implements Comparator<Class<?>>
{
	@Override
	public int compare(Class<?> c1, Class<?> c2)
	{
		if( c1.equals(c2) )
			return 0;
		if ( c1.isAssignableFrom(c2) )
		{
			// c1 is the same or super class of c2,
			// put c1 after c2
			return 1;
		}
		else if ( c2.isAssignableFrom(c1) )
		{
			// c2 is the same or super class of c1,
			// put c1 before c2
			return -1;
		}
		else
		{
			// c1 and c2 doesn't have direct class hierarchy 
			// relationship, they might have share the same parent,
			// sort by class full name
			return c1.getCanonicalName().compareTo(c2.getCanonicalName());
		}
	}
}
