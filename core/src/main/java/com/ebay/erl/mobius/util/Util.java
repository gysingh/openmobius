package com.ebay.erl.mobius.util;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;

import com.ebay.erl.mobius.core.collection.BigTupleList;
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
public class Util 
{
	public static final String[] ZERO_SIZE_STRING_ARRAY = new String[0];
	
	private static final Map<String, Class<?> > _CLASS_MAPPING = new HashMap<String, Class<?>>();
	
	/**
	 * Get the {@link Class} reference by the given
	 * <code>fullClassName</code>.
	 */
	public static Class<?> getClass(String fullClassName)
	{
		Class<?> clazz = null;
		synchronized(_CLASS_MAPPING)
		{			
			if( (clazz=_CLASS_MAPPING.get(fullClassName))==null )
			{
				try 
				{
					clazz = Class.forName(fullClassName);
					_CLASS_MAPPING.put(fullClassName, clazz);
				} 
				catch (ClassNotFoundException e) 
				{
					throw new RuntimeException(e);
				}
			}
		}
		return clazz;
	}
	
	/**
	 * Get the instance by the given <code>fullClassName</code>.
	 */
	public static Object newInstance(String fullClassName)
	{
		try 
		{
			return getClass(fullClassName).newInstance();
		}
		catch (InstantiationException e) 
		{
			throw new RuntimeException(e);
		}
		catch (IllegalAccessException e) 
		{
			throw new RuntimeException(e);
		}
	}
	
	public static Iterable<Tuple> crossProduct(Configuration conf, Reporter reporter, List<BigTupleList> datasets)
		throws IOException
	{
		BigTupleList[] data = new BigTupleList[datasets.size()];
		for( int i=0;i<datasets.size();i++)
		{
			data[i] = datasets.get(i);
		}
		return crossProduct(conf, reporter, data);
	}
	
	
	/**
	 * Perform cross product for the given <code>datasets</code>
	 */
	public static Iterable<Tuple> crossProduct(Configuration conf, Reporter reporter, Iterable<Tuple>... datasets)
		throws IOException
	{
		// no need to cross product if there is only one dataset
		if( datasets.length==1 )
			return datasets[0];
		
		BigTupleList result = new BigTupleList(reporter);
		result.addAll(datasets[0]);
		
		for( int i=1;i<datasets.length;i++ )
		{
			Iterable<Tuple> dataset1 = result;
			Iterable<Tuple> dataset2 = datasets[i];
			
			if( dataset2!=null )
			{			
				BigTupleList temp = new BigTupleList(reporter);
				Iterator<Tuple> it1 = dataset1.iterator();
				while( it1.hasNext() )
				{
					Tuple rowFromDS1 = it1.next();
					
					Iterator<Tuple> it2 = dataset2.iterator();
					while( it2.hasNext() )
					{
						Tuple merged = Tuple.merge(rowFromDS1, it2.next());
						temp.add(merged);
					}
					close(it2);
				}
				close(it1);
				result.clear();
				result = null;
				result = temp;
			}
		}
		return result;
	}
	
	public static Iterable<Tuple> inMemoryCrossProduct(Iterable<Tuple>... datasets)
	{
		// no need to cross product if there is only one dataset
		if( datasets.length==1 )
			return datasets[0];
		
		List<Tuple> result = new ArrayList<Tuple>();
		for( Tuple aTuple:datasets[0] )
		{
			result.add(aTuple);
		}
		
		for( int i=1;i<datasets.length;i++ )
		{
			Iterable<Tuple> dataset1 = result;
			Iterable<Tuple> dataset2 = datasets[i];
			
			List<Tuple> temp = new ArrayList<Tuple>();
			for( Tuple rowFromDS1:dataset1 )
			{
				for( Tuple rowFromDS2:dataset2 )
				{
					temp.add(Tuple.merge(rowFromDS1, rowFromDS2));
				}
			}
			result.clear();
			result = null;
			result = temp;
		}
		return result;
	}
	
	
	/**
	 * Merge the given <code>confs</code> into ones.
	 * <p>
	 * 
	 * The value from same property key in the later
	 * configuration objects in the <code>confs</code>
	 * will override the previous one.  
	 * 
	 * @return a new Configuration that has all the values
	 * in the given <code>confs</code> list.
	 */
	public static Configuration merge(Configuration... confs)
	{
		Configuration newConf = new Configuration (false);
		for ( Configuration aConf : confs )
		{
			Iterator<Entry<String, String>> it = aConf.iterator ();
			while ( it.hasNext () )
			{
				Entry<String, String> anEntry = it.next ();
				newConf.set (anEntry.getKey (), anEntry.getValue ());
			}
		}
		return newConf;
	}
	
	
	
	public boolean equalContent(File f1, File f2)
		throws IOException
	{
		if( f1.length()!=f2.length() )
			return false;
		
		BufferedReader br1 = null;
		BufferedReader br2 = null;
		try
		{
			br1 = new BufferedReader(new FileReader(f1));
			br2 = new BufferedReader(new FileReader(f2));
			
			String nl1 = null;
			String nl2 = null;
			while( true )
			{
				nl1 = br1.readLine();
				nl2 = br1.readLine();
				
				if( nl1!=null && nl2!=null )
				{
					if( !nl1.equals(nl2) )
					{
						return false;
					}
				}
				else if( nl1 ==null && nl2==null )
				{
					// reach EOF same time, and not difference so far
					return true;
				}
				else
				{
					// one of them is EOF, but the other is not.
					return false;
				}
			}
		}finally
		{
			try{ if (br1!=null) br1.close();}catch(Throwable e){}
			try{ if (br2!=null) br2.close();}catch(Throwable e){}
		}
	}
	
	public static int findBoundary(Object[] sorted, Object x, Comparator<Object> comparator, boolean isUpper){
		int start = 0;
		int end = sorted.length-1;
		
		while(start<=end)
		{
			int mid = (start+end)/2;
			int diff = comparator.compare(sorted[mid], x);
			if( diff==0 )
			{
				if( isUpper )
				{
					if( mid==sorted.length-1 )// already at the end of the array
						return mid;
					else
					{
						if (comparator.compare(sorted[mid+1], x)>0 )
						{
							// the next element is greater than x, 
							// found the upper bound
							return mid;
						}
						else
						{
							// the next element is same as x, move
							// the start to mid+1
							start = mid+1;
						}
					}
				}// end of upper bound
				else
				{
					if( mid==0 )// already at the begin of the array
						return mid;
					else
					{
						if (comparator.compare(sorted[mid-1], x)<0 )
						{
							// the previous element is smaller than x, 
							// found the lower bound
							return mid;
						}
						else
						{
							// the previous element is same as x, move
							// the end to mid-1
							end = mid-1;
						}
					}
				}
			}
			else if( diff>0 )
			{
				end = mid-1;
			}
			else
			{
				start = mid+1;
			}
		}
		
		return -1;
	}
	
	public static int findUpperBound(Object[] sorted, Object x, Comparator<Object> comparator)
	{
		return findBoundary(sorted, x, comparator, true);
	}
	
	public static int findLowerBound(Object[] sorted, Object x, Comparator<Object> comparator)
	{
		return findBoundary(sorted, x, comparator, false);
	}
	
	public static int findRepeatTimes(Object[] sorted, Object x, Comparator<Object> comparator)
	{
		int upper = findUpperBound(sorted, x, comparator);
		if( upper>=0 )
		{
			return upper-findLowerBound(sorted, x, comparator)+1;
		}
		return -1;
	}
	
	@SuppressWarnings("unchecked")
	public static <T> List<T> findByType(List<? super T> list, Class<T> type)
	{
		if( list==null )
			throw new NullPointerException("<list parameter cannot be null");
		
		if( type==null )
			throw new NullPointerException("type parameter cannot be null");
		
		List<T> subclasses = new ArrayList<T>();
		for( Object e:list )
		{
			
			if( type.isAssignableFrom(e.getClass()) )
			{
				subclasses.add((T)e);
			}
		}
		return subclasses;
	}
	
	@SuppressWarnings("unchecked")
	public static <U, T extends U> T[] findByType(U[] list, Class<T> type)
	{
		List<T> result = findByType(Arrays.asList(list), type);
		T[] t = (T[])Array.newInstance(type, result.size());
		for( int i=0;i<result.size();i++ )
			t[i] = result.get(i);
		return t;
	}
	
	
	public static <E> void close(Iterator<E> it)
		throws IOException
	{
		if( it!=null && it instanceof Closeable ){
			((Closeable)it).close();
		}
	}
}
