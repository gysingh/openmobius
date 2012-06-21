package com.ebay.erl.mobius.core.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.ebay.erl.mobius.core.collection.CaseInsensitiveTreeMap;

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
public class TupleTest 
{
	private static final Log LOGGER = LogFactory.getLog(TupleTest.class);
	
	@Test
	public void test_comparator()
	{
		Tuple t1 = new Tuple();		
		Tuple t2 = new Tuple();
		
		
		// comparing short
		t1.put("C1", (byte)12);
		t2.put("C1", (byte)10);
		assertTrue(t1.compareTo(t2)>0);
		assertTrue(t2.compareTo(t1)<0);
		
		// comparing short
		t1.put("C1", (short)12);
		t2.put("C1", (short)10);
		assertTrue(t1.compareTo(t2)>0);
		assertTrue(t2.compareTo(t1)<0);
		
		// comparing integer
		t1.put("C1", (int)10);
		t2.put("C1", (int)10);
		assertTrue(t1.compareTo(t2)==0);
		assertFalse(t2.compareTo(t1)<0);
		assertFalse(t1.compareTo(t2)>0);
		
		// comparing long
		t1.put("C1", 200000000L);
		t2.put("C1", 300000000L);
		assertTrue(t1.compareTo(t2)!=0);
		assertFalse(t1.compareTo(t2)>0);
		assertTrue(t1.compareTo(t2)<0);
		
		// comparing float
		t1.put("C1", 1.2F);
		t2.put("C1", 1.1F);
		assertTrue(t1.compareTo(t2)>0);
		assertTrue(t2.compareTo(t1)<0);
		
		// comparing double
		t1.put("C1", 1.7D);
		t2.put("C1", 1.5D);
		assertTrue(t1.compareTo(t2)>0);
		assertTrue(t2.compareTo(t1)<0);
		
		// comparing string
		t1.put("C1", "AA");
		t2.put("C1", "AB");
		assertTrue(t1.compareTo(t2)<0);
		assertTrue(t2.compareTo(t1)>0);
		
		// comparing date
		t1.put("C1", java.sql.Date.valueOf("2011-01-01"));
		t2.put("C1", java.sql.Date.valueOf("2011-01-02"));
		assertTrue(t1.compareTo(t2)<0);
		assertTrue(t2.compareTo(t1)>0);
		
		// comparing Timestamp
		Timestamp ts1 = Timestamp.valueOf("2011-12-31 12:30:42");
		Timestamp ts2 = Timestamp.valueOf("2011-12-31 12:30:12");		
		t1.put("C1", ts1);
		t2.put("C1", ts2);
		assertTrue(t1.compareTo(t2)>0);
		assertTrue(t2.compareTo(t1)<0);
		
		
		// comparing Time
		Time tt1 = Time.valueOf("12:30:42");
		Time tt2 = Time.valueOf("12:30:12");		
		t1.put("C1", tt1);
		t2.put("C1", tt2);
		assertTrue(t1.compareTo(t2)>0);
		assertTrue(t2.compareTo(t1)<0);
		
		// comparing boolean
		t1.put("C1", true);
		t2.put("C1", false);
		assertTrue(t1.compareTo(t2)>0);
		assertTrue(t2.compareTo(t1)<0);
		
		// comparing string map
		CaseInsensitiveTreeMap m1 = new CaseInsensitiveTreeMap();
		CaseInsensitiveTreeMap m2 = new CaseInsensitiveTreeMap();
		t1.put("C1", m1);
		t2.put("C1", m2);
		assertTrue(t1.compareTo(t2)==0);
		
		m1.put("K1", "V1");
		assertTrue(t1.compareTo(t2)>0); // m1 is not empty, m2 is empty
		
		m2.put("K1", "V1");
		assertTrue(t1.compareTo(t2)==0);// m1 and m2 both are not empty and same value
		
		m1.put("K1", "V2");
		assertTrue(t1.compareTo(t2)>0);// m1 m2 has same key but different value
		
		m1.put("K1", "V1"); 
		m1.put("K2", "V2");
		assertTrue(t1.compareTo(t2)>0);// m1 m2 both has same K1 and V1, but m1 has more k-v pair
		assertTrue(t2.compareTo(t1)<0);
		
		
		
		// comparing WritableComparable
		t1.put("C1", new Text("AC"));
		t2.put("C1", new Text("AA"));
		assertTrue(t1.compareTo(t2)>0);
		assertTrue(t2.compareTo(t1)<0);
		
		// comparing serializable type
		t1.put("C1", new BigDecimal(123L));
		t2.put("C1", new BigDecimal(456L));
		assertTrue(t1.compareTo(t2)<0);
		assertTrue(t2.compareTo(t1)>0);
		
		// comparing null type
		t1.put("C1", this.getNull());
		t2.put("C1", new BigDecimal(456L));
		
		assertTrue(t1.compareTo(t2)<0);
		assertTrue(t2.compareTo(t1)>0);
		
		t2.put("C1", this.getNull());
		assertTrue(t1.compareTo(t2)==0);
		
		// comparing different numerical type
		t1.put("C1", 1.2F);
		t2.put("C1", 1);
		assertTrue(t1.compareTo(t2)>0);		
	}
	
	@Test
	public void insert_test()
	{
		this._assertp_tuple(this.prepareTupe());
	}
	
	/**
	 * test serialization and de-serialization
	 * 
	 * @throws IOException
	 */
	@Test
	public void test_serde()
		throws IOException
	{
		Tuple t = this.prepareTupe();
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bos);
		
		t.write(out);
		
		out.flush();
		out.close();
		
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
		
		Tuple nt = new Tuple();
		nt.readFields(in);
		
		// ordering doesn't matter
		nt.setSchema(new String[]{"C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10", "C11", "C12"});
		
		this._assertp_tuple(nt);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void test_non_exist_column()
	{
		Tuple t = this.prepareTupe();
		t.get("NA");
	}
	
	@Test
	public void wrong_type_checking()
	{
		Tuple t = this.prepareTupe();
		
		// the following should be fine
		t.getInt("C1");	// same type
		t.getFloat("C1"); // int to float
		t.getLong("C1"); // int to long
		t.getDouble("C1"); // int to double
		
		t.getInt("C2");
		t.getFloat("C2");
		t.getLong("C2");
		t.getDouble("C2");
		
		t.getInt("C3");
		t.getFloat("C3");
		t.getLong("C3");
		t.getDouble("C3");
		
		t.getInt("C4");
		t.getFloat("C4");
		t.getLong("C4");
		t.getDouble("C4");
		
		
		
		try
		{
			t.getInt("c6");
			fail();
		}catch(ClassCastException e)
		{
			LOGGER.debug(e);
		}
		
		try
		{
			t.getInt("c7");
			fail();
		}catch(ClassCastException e)
		{
			LOGGER.debug(e);
		}
		
		try
		{
			t.getInt("c8");
			fail();
		}catch(ClassCastException e)
		{
			LOGGER.debug(e);
		}
		
		try
		{
			t.getInt("c9");
			fail();
		}catch(ClassCastException e)
		{
			LOGGER.debug(e);
		}
		
		try
		{
			t.getInt("c10");
			fail();
		}catch(ClassCastException e)
		{
			LOGGER.debug(e);
		}
		
		try
		{
			t.getInt("c11");
			fail();
		}catch(ClassCastException e)
		{
			LOGGER.debug(e);
		}
		
		t.getInt("c12"); // byte to int, should pass
	}
	
	
	
	@SuppressWarnings("unchecked")
	private void _assertp_tuple(Tuple t)
	{
		assertEquals(1, t.getInt("C1"));
		assertEquals(1.2F, t.getFloat("C2"));
		assertEquals(1.3D, t.getDouble("C3"));
		assertEquals(1L, t.getLong("C4"));
		assertEquals("s5", t.getString("C5"));
		assertEquals("s5", t.get("C5"));
		
		
		Map<String, String> m = (Map<String, String>)t.get("C6");		
		assertEquals(m.get("k1"), "v1");
		assertEquals(m.get("k2"), "v2");
		
		// using map style ID to access the value directly
		assertTrue(t.get("C6.k1").equals("v1"));
		assertTrue(t.get("c6.K2").equals("v2"));
		
		t.put("C6.k1", "NV1");
		assertTrue(t.get("c6.k1").equals("NV1"));
		t.put("C6.k3", "NV3");
		assertTrue(t.get("c6.k3").equals("NV3"));
		assertNull(t.get("c6.k4"));
		t.put("C6.k3", "1");
		assertTrue(t.getInt("c6.k3")==1);
		
		assertEquals(new Text("hello"), t.get("C7"));		
		assertEquals(new BigDecimal(1000), t.get("C8"));
		
		assertNull(t.get("C9"));
		
		assertEquals(false, t.getBoolean("C10"));
		
		assertEquals(java.sql.Date.valueOf("2000-12-12"), t.getDate("C11"));
		
		assertEquals((byte)0x09, t.getByte("C12"));
	}
	
	private Tuple prepareTupe()
	{
		Tuple t = new Tuple();
		
		t.put("C1", 1);
		t.put("C2", 1.2F);
		t.put("C3", 1.3D);
		t.put("C4", 1L);
		t.put("C5", "s5");
		
		CaseInsensitiveTreeMap m = new CaseInsensitiveTreeMap();
		m.put("k1", "v1");
		m.put("k2", "v2");
		
		t.put("C6", m);
		
		Text txt = new Text();
		txt.set("hello");
		t.put("C7", txt);
		
		BigDecimal serializable = new BigDecimal(1000);
		t.put("C8", serializable);
		
		t.put("C9", getNull());
		
		t.put("C10", false);
		
		t.put("C11", java.sql.Date.valueOf("2000-12-12"));
		
		t.put("C12", (byte)0x09);
		
		
		return t;
	}
	
	private Date getNull()
	{
		return null;
	}
}
