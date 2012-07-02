package com.ebay.erl.mobius.core.model;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ArrayTest 
{
	@Test
	public void test()
		throws IOException
	{
		Object[] data = new Object[]{
				Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, Float.MAX_VALUE, Double.MAX_VALUE,
				"StRing", Boolean.TRUE, 
				java.sql.Date.valueOf("2012-12-11"),
				java.sql.Timestamp.valueOf("2012-01-02 23:59:10"),
				java.sql.Time.valueOf("23:00:10"),
				NullWritable.get(),
				new Text("text writable")
		};
		
		Array array = new Array();
		
		for( Object obj:data )
		{
			array.add(obj);
		}
		
		int counter = 0;
		for( Object actual:array )
		{
			Object expected = data[counter++];
			
			assertTrue(expected.getClass().equals(actual.getClass()));
			assertTrue(expected.equals(actual));
		}
		
		// serialize array, deserialize it back, then test it again
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		array.write(dos);
		dos.flush();
		dos.close();
		
		byte[] binary = bos.toByteArray();
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(binary));
		Array deseralized = new Array();
		deseralized.readFields(dis);
		counter = 0;
		for( Object actual:deseralized )
		{
			Object expected = data[counter++];
			
			assertTrue(expected.getClass().equals(actual.getClass()));
			assertTrue(expected.equals(actual));
		}
	}
}
