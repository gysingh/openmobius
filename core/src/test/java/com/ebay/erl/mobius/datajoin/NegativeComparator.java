package com.ebay.erl.mobius.datajoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

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
public class NegativeComparator extends WritableComparator {

	public NegativeComparator() {
		super(WritableComparable.class);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		return -super.compare(a, b);
	}
}
