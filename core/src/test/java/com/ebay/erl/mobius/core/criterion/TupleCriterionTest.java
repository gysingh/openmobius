package com.ebay.erl.mobius.core.criterion;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TreeMap;

import org.junit.Test;

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
public class TupleCriterionTest 
{
	@Test
	public void testSimpleCriterion()
	{
		Calendar c = Calendar.getInstance();
		c.set(Calendar.YEAR, 2011);
		c.set(Calendar.MONTH, Calendar.JANUARY);
		c.set(Calendar.DAY_OF_MONTH, 1);
		c.set(Calendar.HOUR_OF_DAY, 23);
		c.set(Calendar.MINUTE, 59);
		c.set(Calendar.SECOND, 59);
		c.set(Calendar.MILLISECOND, 0);
		
		TreeMap<String, String> m = new TreeMap<String, String>();
		m.put("K", "V");
		
		
		Tuple t = new Tuple();
		t.put(Byte.class.getName(), (byte)0x0F);		
		t.put(Short.class.getName(), (short)1);
		t.put(Integer.class.getName(), (int)2);
		t.put(Long.class.getName(), 3L);
		t.put(Float.class.getName(), 4.1F);
		t.put(Double.class.getName(), 5.1D);
		
		t.put(Boolean.class.getName(), false);
		
		t.put(String.class.getName(), "string");
		t.put(java.sql.Date.class.getName(), java.sql.Date.valueOf("2011-01-01"));
		t.put(java.sql.Timestamp.class.getName(), java.sql.Timestamp.valueOf("2012-01-01 13:05:01"));
		t.put(java.sql.Time.class.getName(), java.sql.Time.valueOf("13:05:01"));
		
		// test numbers
		testNumber(Byte.class.getName(), (byte)0x0F, t);
		testNumber(Short.class.getName(), (short)1, t);
		testNumber(Integer.class.getName(), (int)2, t);
		testNumber(Long.class.getName(), 3L, t);
		testNumber(Float.class.getName(), 4.1F, t);
		testNumber(Double.class.getName(), 5.1, t);
		
		// test boolean
		assertTrue(TupleRestrictions.eq(Boolean.class.getName(), false).accept(t, null));
		assertFalse(TupleRestrictions.ne(Boolean.class.getName(), false).accept(t, null));
		
		// test string
		assertTrue(TupleRestrictions.eq(String.class.getName(), "string").accept(t, null));
		assertFalse(TupleRestrictions.ne(String.class.getName(), "string").accept(t, null));
		assertTrue(TupleRestrictions.gt(String.class.getName(), "ssring").accept(t, null));
		assertTrue(TupleRestrictions.ge(String.class.getName(), "ssring").accept(t, null));
		assertTrue(TupleRestrictions.lt(String.class.getName(), "suring").accept(t, null));
		assertTrue(TupleRestrictions.le(String.class.getName(), "suring").accept(t, null));
		
		// test date
		assertTrue(TupleRestrictions.eq(Date.class.getName(), java.sql.Date.valueOf("2011-01-01")).accept(t, null));
		assertTrue(TupleRestrictions.ne(Date.class.getName(), java.sql.Date.valueOf("2011-01-02")).accept(t, null));
		assertTrue(TupleRestrictions.ge(Date.class.getName(), java.sql.Date.valueOf("2011-01-01")).accept(t, null));
		assertTrue(TupleRestrictions.gt(Date.class.getName(), java.sql.Date.valueOf("2010-12-31")).accept(t, null));
		assertTrue(TupleRestrictions.le(Date.class.getName(), java.sql.Date.valueOf("2011-01-01")).accept(t, null));
		assertTrue(TupleRestrictions.lt(Date.class.getName(), java.sql.Date.valueOf("2011-01-02")).accept(t, null));
		
		// test time
		assertTrue(TupleRestrictions.eq(Time.class.getName(), java.sql.Time.valueOf("13:05:01")).accept(t, null));
		assertTrue(TupleRestrictions.ne(Time.class.getName(), java.sql.Time.valueOf("13:05:00")).accept(t, null));
		assertTrue(TupleRestrictions.ge(Time.class.getName(), java.sql.Time.valueOf("13:05:01")).accept(t, null));
		assertTrue(TupleRestrictions.gt(Time.class.getName(), java.sql.Time.valueOf("13:05:00")).accept(t, null));
		assertTrue(TupleRestrictions.le(Time.class.getName(), java.sql.Time.valueOf("13:05:01")).accept(t, null));
		assertTrue(TupleRestrictions.lt(Time.class.getName(), java.sql.Time.valueOf("13:05:02")).accept(t, null));
		
		// test timestamp
		
	}
	
	private void testNumber(String columnName, Number original, Tuple t)
	{
		assertTrue(TupleRestrictions.eq(columnName, original).accept(t, null));
		assertFalse(TupleRestrictions.ne(columnName, original).accept(t, null));
		
		assertFalse(TupleRestrictions.gt(columnName, (original.doubleValue() + 1) ).accept(t, null));
		assertTrue(TupleRestrictions.ge(columnName, original).accept(t, null));
		assertTrue(TupleRestrictions.lt(columnName, (original.doubleValue() + 1) ).accept(t, null));
		assertTrue(TupleRestrictions.le(columnName, original).accept(t, null));
		
		ArrayList<Double> numbers = new ArrayList<Double>();
		numbers.add(original.doubleValue());
		numbers.add(original.doubleValue()+5);
		numbers.add(original.doubleValue()-5);
		
		assertTrue(TupleRestrictions.withinNumber(columnName, numbers).accept(t, null));
		assertFalse(TupleRestrictions.notWithinNumber(columnName, numbers).accept(t, null));
	}
	
	
}
