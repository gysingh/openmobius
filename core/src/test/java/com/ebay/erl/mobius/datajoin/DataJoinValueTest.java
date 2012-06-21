package com.ebay.erl.mobius.datajoin;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.ebay.erl.mobius.core.datajoin.DataJoinValue;

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
public class DataJoinValueTest {

	@Test
	public void testConstructor() {
		DataJoinValue djvalue = new DataJoinValue("1", new Text("2"));
		
		Assert.assertEquals("1", djvalue.getDatasetID());
		Assert.assertEquals(new Text("2"), djvalue.getValue());
		
	}
	
	@Test
	public void testCompare() {
		DataJoinValue djvalue1 = new DataJoinValue("1", new Text("1"));
		DataJoinValue djvalue1_1 = new DataJoinValue("1", new Text("1"));
		DataJoinValue djvalue2 = new DataJoinValue("1", new Text("2"));
		
		Assert.assertEquals(-1, djvalue1.compareTo(djvalue2));
		Assert.assertEquals(1, djvalue2.compareTo(djvalue1));
		Assert.assertEquals(0, djvalue1.compareTo(djvalue1_1));
		Assert.assertEquals(0, djvalue1_1.compareTo(djvalue1));
	}
}
