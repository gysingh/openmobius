package com.ebay.erl.mobius.util;

import java.io.Serializable;
import java.util.Comparator;

/**
 * 
 * A serializable comparator.
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
 * @param <T>  the type of objects that may be compared by this comparator
 */
public interface SerializableComparator<T> extends Serializable, Comparator<T> 
{
}
