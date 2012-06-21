package com.ebay.erl.mobius.datajoin;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.ebay.erl.mobius.core.datajoin.DataJoinKey;
import com.ebay.erl.mobius.core.model.Tuple;

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
public class DataJoinKeyTest {

	@Test
	public void testConstructor() {
		DataJoinKey djKey = new DataJoinKey("1", new Text("test value"));
		
		Assert.assertEquals("1", djKey.getDatasetID());
		Assert.assertEquals(new Text("test value"), djKey.getKey());
		//Assert.assertEquals(null, djKey.getSortKeyword());
		//Assert.assertEquals(Class.class, djKey.getSortComparator());
		
		djKey = new DataJoinKey(new Text("1"), new Text("test value"), new Text("2"), DataJoinKey.Comparator.class);
		
		Assert.assertEquals("1", djKey.getDatasetID());
		Assert.assertEquals(new Text("test value"), djKey.getKey());
		//Assert.assertEquals(new Text("2"), djKey.getSortKeyword());
		//Assert.assertEquals(DataJoinKey.Comparator.class, djKey.getSortComparator());
		
		djKey = new DataJoinKey(new Text("1"), new Text("test value"), new Text("2"), null);
		
		Assert.assertEquals("1", djKey.getDatasetID());
		Assert.assertEquals(new Text("test value"), djKey.getKey());
		//Assert.assertEquals(new Text("2"), djKey.getSortKeyword());
		//Assert.assertEquals(Class.class, djKey.getSortComparator());
	}
	
	
	@Test
	public void testCompare() {
		DataJoinKey djKey1 = new DataJoinKey("1", new Text("1"));
		DataJoinKey djKey1_1 = new DataJoinKey("1", new Text("1"));
		DataJoinKey djKey2 = new DataJoinKey("1", new Text("2"));
		
		Assert.assertEquals(-1, djKey1.compareTo(djKey2));
		Assert.assertEquals(1, djKey2.compareTo(djKey1));
		Assert.assertEquals(0, djKey1.compareTo(djKey1_1));
		
		Assert.assertEquals(-1, djKey1.compare(djKey1, djKey2));
		Assert.assertEquals(1, djKey1.compare(djKey2, djKey1));
		Assert.assertEquals(0, djKey1.compare(djKey1, djKey1_1));
		
		djKey1 = new DataJoinKey(new Text("1"), new Text("1"), new Text("1"), null);
		djKey1_1 = new DataJoinKey(new Text("1"), new Text("1"), new Text("2"), null);
		djKey2 = new DataJoinKey(new Text("1"), new Text("2"), new Text("1"), null);
		
		Assert.assertEquals(-1, djKey1.compareTo(djKey2));
		Assert.assertEquals(1, djKey2.compareTo(djKey1));
		Assert.assertEquals(0, djKey1.compareTo(djKey1_1));
		Assert.assertEquals(0, djKey1_1.compareTo(djKey1));
		
		Assert.assertEquals(-1, djKey1.compare(djKey1, djKey2));
		Assert.assertEquals(1, djKey1.compare(djKey2, djKey1));
		Assert.assertEquals(0, djKey1.compare(djKey1, djKey1_1));
		Assert.assertEquals(0, djKey1.compare(djKey1_1, djKey1));
		
		djKey1 = new DataJoinKey(new Text("1"), new Text("1"), new Text("1"), NegativeComparator.class);
		djKey1_1 = new DataJoinKey(new Text("1"), new Text("1"), new Text("2"), NegativeComparator.class);
		djKey2 = new DataJoinKey(new Text("1"), new Text("2"), new Text("1"), NegativeComparator.class);
		
		Assert.assertEquals(-1, djKey1.compareTo(djKey2));
		Assert.assertEquals(1, djKey2.compareTo(djKey1));
		Assert.assertEquals(0, djKey1.compareTo(djKey1_1));
		Assert.assertEquals(0, djKey1_1.compareTo(djKey1));
		
		Assert.assertEquals(-1, djKey1.compare(djKey1, djKey2));
		Assert.assertEquals(1, djKey1.compare(djKey2, djKey1));
		Assert.assertEquals(0, djKey1.compare(djKey1, djKey1_1));
		Assert.assertEquals(0, djKey1.compare(djKey1_1, djKey1));
	}
	
	@Test
	public void testHash() {
		DataJoinKey djKey1 = new DataJoinKey("1", new Text("1"));
		DataJoinKey djKey1_1 = new DataJoinKey("1", new Text("1"));
		DataJoinKey djKey2 = new DataJoinKey("1", new Text("2"));
		
		Assert.assertEquals(djKey1.hashCode(), djKey1_1.hashCode());
		Assert.assertNotSame(djKey1.hashCode(), djKey2.hashCode());
	}
	
	@Test
	public void testSerDe()
		throws IOException
	{
		Tuple t1 = new Tuple();
		t1.put("k1", "v1");
		
		Tuple t2 = new Tuple();
		t2.put("k2", "v2");
		
		DataJoinKey k1 = new DataJoinKey("1", t1);
		DataJoinKey k2 = new DataJoinKey("1", t2);
		
		ByteArrayOutputStream b1 = new ByteArrayOutputStream();
		ByteArrayOutputStream b2 = new ByteArrayOutputStream();
		
		DataOutputStream out1 = new DataOutputStream(b1);
		DataOutputStream out2 = new DataOutputStream(b2);
		
		k1.write(out1);
		k2.write(out2);
		
		out1.flush();
		out2.flush();
		
		byte[] ba1 = b1.toByteArray();
		byte[] ba2 = b2.toByteArray();
		
		DataInputStream in1 = new DataInputStream(new ByteArrayInputStream(ba1));
		DataInputStream in2 = new DataInputStream(new ByteArrayInputStream(ba2));
		
		DataJoinKey k_1 = new DataJoinKey();
		k_1.readFields(in1);
		
		DataJoinKey k_2 = new DataJoinKey();
		k_2.readFields(in2);
		
		Assert.assertEquals(-1, k_1.compareTo(k_2));
		Assert.assertEquals(-1, k_1.compare(ba1, 0, ba1.length, ba2, 0, ba2.length));
		
	}
	
}
