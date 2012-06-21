package com.ebay.erl.mobius.datajoin;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.ebay.erl.mobius.core.datajoin.DataJoinKey;
import com.ebay.erl.mobius.core.datajoin.DataJoinKeyPartitioner;

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
public class DataJoinKeyPartitionerTest {

	@Test
	public void testPartition() {
		DataJoinKeyPartitioner partitioner = new DataJoinKeyPartitioner();
		
		DataJoinKey djKey1 = new DataJoinKey("1", new Text("test value"));
		DataJoinKey djKey2 = new DataJoinKey("1", new Text("test value"));
		DataJoinKey djKey3 = new DataJoinKey("1", new Text("other test value"));
		
		System.out.println(partitioner.getPartition(djKey1, null, 999));
		System.out.println(partitioner.getPartition(djKey2, null, 999));
		System.out.println(partitioner.getPartition(djKey3, null, 999));
		
		Assert.assertEquals(partitioner.getPartition(djKey1, null, 999), 
				partitioner.getPartition(djKey2, null, 999));
		
		Assert.assertNotSame(partitioner.getPartition(djKey1, null, 999), 
				partitioner.getPartition(djKey3, null, 999));
	}
}
