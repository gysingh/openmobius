package com.ebay.erl.mobius.core.collection;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.zip.GZIPInputStream;

import org.junit.Test;

import com.ebay.erl.mobius.core.MobiusJob;
import com.ebay.erl.mobius.core.MobiusJobRunner;
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
public class BigTupleListTest extends MobiusJob
{	
	private static final long serialVersionUID = 3229032091821609737L;
	
	@Test
	public void test()
	{
		try
		{
			MobiusJobRunner.run(new BigTupleListTest(), new String[]{});
		}catch(Throwable e)
		{
			e.printStackTrace();
			fail();
		}
	}

	

	@Override
	public int run(String[] args) throws Exception 
	{
		//BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(new File("C:/Users/chichiu/Downloads/soj_D20120122_000.txt.1000"))), "UTF-8"));
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("C:/Users/chichiu/Downloads/soj_D20120122_000.txt.1000")), "UTF-8"));
		String nl = null;
		
		Comparator<Tuple> c = new Comparator<Tuple>()
		{
			@Override
			public int compare(Tuple o1, Tuple o2) 
			{
				int diff = o1.getString("C6").compareTo(o2.getString("C6"));// time
				if( diff==0 )
				{
					diff = o1.getString("C0").compareTo(o2.getString("C0"));//GUID
					if( diff==0 )
					{
						diff = o1.getInt("C3").compareTo(o2.getInt("C3"));;
					}
				}
				return diff;
			}
			
		};
		
		BigTupleList lst = new BigTupleList(c, null);
		long start = System.currentTimeMillis();
		long inserted = 0L;
		while( (nl=br.readLine())!=null )
		{
			if( inserted>500000)
				break;
			
			String[] data = nl.split("\t");
			Tuple t = new Tuple();
			for( int i=0;i<10;i++)
			{
				t.put("C"+i, data[i]);
			}
			lst.add(t);
			inserted++;
			if( inserted%50000==0)
			{
				System.out.println(inserted);
				CloseableIterator<Tuple> it = lst.iterator();
				while( it.hasNext() ){
					it.next();
				}
				it.close();
				lst.clear();
			}
		}
		long end = System.currentTimeMillis();
		System.out.println(((end-start)/1000)+" seconds");
		
		System.out.println("inserted:"+inserted);
		
		
		start = System.currentTimeMillis();
		long read = 0;
		CloseableIterator<Tuple> it = lst.iterator();
		while( it.hasNext() )
		{
			it.next();
			read++;
			if( inserted%50000==0)
				System.out.println(read);
		}
		it.close();
		System.out.println("read:"+read);
		end = System.currentTimeMillis();
		System.out.println(((end-start)/1000)+" seconds");
		
		if( read!=inserted)
			throw new RuntimeException();
		
		return 0;
	}
	
	
	public static void main(String[] arg)
		throws Throwable
	{
		BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(new File("C:/Users/chichiu/Downloads/soj_D20120122_000.txt.gz")))));
		String newLine;
		int counts = 0;
		final int MAX = 100000;
		
		BigTupleList btl = new BigTupleList(null);
		
		while( (newLine=br.readLine())!=null ){
			String[] data = newLine.split("\t");
			Tuple t = new Tuple();
			for( int i=0;i<10;i++)
				t.put("T"+i, data[i]);
			
			btl.add(t);
			counts++;
			
			if( counts==MAX ){
				System.out.println(counts);
				System.out.println("Prepare to iterate");
				CloseableIterator<Tuple> it = btl.iterator();
				System.out.println("Iterator return");
				int c = 0;
				while( it.hasNext() ){
					it.next();
					c++;
				}
				it.close();
				counts = 0;
			}
		}
	}
}
