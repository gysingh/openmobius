package com.ebay.erl.mobius.core.model;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;

import com.ebay.erl.mobius.core.datajoin.DataJoinKey;
import com.ebay.erl.mobius.core.sort.Sorter;
import com.ebay.erl.mobius.core.sort.Sorter.Ordering;

/**
 * Comparator for comparing values from two columns (object1 and 
 * object2 in the compare method).
 * <p>
 * 
 * This class supports comparing exchangeable type values, such as
 * comparing a number in string format to a long.
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
 */
@SuppressWarnings("unchecked")
public class TupleColumnComparator 
{	
	private static final Log LOGGER = LogFactory.getLog(TupleColumnComparator.class);
	
	private byte type_for_object1;	
	
	private byte type_for_object2;
	
	private static TreeMap<String, Integer> _IDX_MAPPING;
	
	
	public void setType(byte type_for_object1, byte type_for_object2)
	{
		this.type_for_object1 = type_for_object1;
		this.type_for_object2 = type_for_object2;
	}
	
	
	
	/**
	 * to be called in {@link DataJoinKey}
	 */	
	public int compareKey(WritableComparable key1, WritableComparable key2, Sorter[] sorters, Configuration conf)		
	{
		if( sorters.length==0 )
		{
			return key1.compareTo(key2);
		}
		else
		{
			// when this method is called, and sorters is not null,
			// key1 and key2 must be Tuple instances.
			Tuple k1 = (Tuple)key1;
			Tuple k2 = (Tuple)key2;	
			
			
			// the sorting priority is the same as the sorters 
			// array
			for( int i=0;i<sorters.length;i++ )
			{
				Sorter aSorter 			= sorters[i];				
				
				String columnName		= aSorter.getColumn();
				boolean forceNumeric 	= aSorter.forceSortNumerically();
				
				//////////////////////////////
				// get value from k1, k2
				//////////////////////////////
				Object v1 = this.getValue(k1, columnName, forceNumeric, sorters);				
				Object v2 = this.getValue(k2, columnName, forceNumeric, sorters);
				
				//////////////////////
				// start to compare
				//////////////////////				
				int result;
				try 
				{
					this.setType(Tuple.getType(v1), Tuple.getType(v2));
					result = this.compare(v1, v2, conf);				
					if( result!=0 )
					{
						// ordering is decided, return the result
						Ordering ordering 	= aSorter.getOrdering();
						switch(ordering)
						{
							case ASC:
								// remain the save
								return result;
							case DESC:
								// reverse the ordering
								return -result;
							default:
								throw new IllegalArgumentException(ordering+" is not a supported ordering.");
						}
					}
				}
				catch (IOException e)
				{					
					throw new RuntimeException("Cannot performe compareKey", e);
				}				
			}
			
		}
		return 0;
	}
	
	
	public int compare(Object object1, Object object2, Configuration conf)
		throws IOException
	{
		int _compare_result = Integer.MAX_VALUE;
		
		ObjectReader reader1 = new ObjectReader(object1, type_for_object1);
		ObjectReader reader2 = new ObjectReader(object2, type_for_object2);
		
		final Object v1 = reader1.getValue();
		final Object v2 = reader2.getValue();
		
		setType(Tuple.getType(v1), Tuple.getType(v2));
		
		if( type_for_object1==type_for_object2 )
		{
			final byte equal_type = type_for_object1;
			final TupleColumnComparator cmp = new TupleColumnComparator();
			cmp.setType(equal_type, equal_type);
			TupleTypeHandler<Integer> equalTypeComparator = new TupleTypeHandler<Integer>(){
					
				@Override
				protected Integer on_boolean() throws IOException {
					return compare( (Boolean)v1, (Boolean)v2);
				}

				@Override
				protected Integer on_byte() throws IOException {
					return compare( (Byte)v1, (Byte)v2);
				}

				@Override
				protected Integer on_byte_array() throws IOException {
					byte[] b1 = (byte[])v1;
					byte[] b2 = (byte[])v2;
					int diff = b1.length-b2.length;
					if( diff==0 )
					{
						// equal size
						for( int i=0;i<b1.length;i++ ){
							diff = compare((Byte)b1[i], (Byte)b2[i]);
							if( diff!=0 )
								return diff;
						}
					}
					return diff;
				}

				@Override
				protected Integer on_date() throws IOException {
					return compare( (java.sql.Date)v1, (java.sql.Date)v2);
				}

				@Override
				protected Integer on_default() throws IOException {
					throw new IllegalArgumentException("Unsupported type ["+String.format("0x%02X", type)+"]");
				}

				@Override
				protected Integer on_double() throws IOException {
					return compare( (Double)v1, (Double)v2);
				}

				@Override
				protected Integer on_float() throws IOException {
					return compare( (Float)v1, (Float)v2);
				}

				@Override
				protected Integer on_integer() throws IOException {
					return compare( (Integer)v1, (Integer)v2);
				}

				@Override
				protected Integer on_long() throws IOException {
					return compare( (Long)v1, (Long)v2);
				}

				@Override
				protected Integer on_null() throws IOException {
					return 0;
				}

				@Override
				protected Integer on_null_writable() throws IOException {
					return 0;
				}

				@Override
				protected Integer on_result_wrapper() throws IOException {
					
					ResultWrapper w1 = (ResultWrapper)v1;
					ResultWrapper w2 = (ResultWrapper)v2;
					return cmp.compare(w1.getCombinedResult(), w2.getCombinedResult(), null);
				}

				@Override
				protected Integer on_serializable() throws IOException {
					return compare( (Comparable)v1, (Comparable)v2);
				}

				@Override
				protected Integer on_short() throws IOException {
					return compare( (Short)v1, (Short)v2);
				}

				@Override
				protected Integer on_string() throws IOException {
					return compare( (String)v1, (String)v2);
				}

				@Override
				protected Integer on_string_map() throws IOException {
					TreeMap<String, String> m1 = (TreeMap<String, String>)v1;
					TreeMap<String, String> m2 = (TreeMap<String, String>)v2;
					return compare(m1, m2);
				}

				@Override
				protected Integer on_time() throws IOException {
					return compare( (java.sql.Time)v1, (java.sql.Time)v2);
				}

				@Override
				protected Integer on_timestamp() throws IOException {
					return compare( (java.sql.Timestamp)v1, (java.sql.Timestamp)v2);
				}

				@Override
				protected Integer on_tuple() throws IOException {
					Tuple t1 = (Tuple)v1;
					Tuple t2 = (Tuple)v2;
					return t1.compareTo(t2);
				}

				@Override
				protected Integer on_writable() throws IOException {
					return compare( (Comparable)v1, (Comparable)v2);
				}			
			};
			
			_compare_result = equalTypeComparator.handle(equal_type);
			
		}
		else
		{
			// different type
			if ( type_for_object1==Tuple.NULL_TYPE || type_for_object2==Tuple.NULL_TYPE )
			{
				// one of them is null type
				_compare_result = (type_for_object1==Tuple.NULL_TYPE) ? -1:1;
			}
			
			else if (Tuple.isNumericalType(type_for_object1) && type_for_object2==Tuple.STRING_TYPE)
			{
				// comparing string with numerical type
				_compare_result = compare( ((Number)v1).doubleValue(), Double.parseDouble(((String)v2)));
			}
			else if (Tuple.isNumericalType(type_for_object2) && type_for_object1==Tuple.STRING_TYPE)
			{
				// comparing string with numerical type
				_compare_result = compare( Double.parseDouble(((String)v1)), ((Number)v2).doubleValue());
			}
			
			else if ( Tuple.isNumericalType(type_for_object1) && Tuple.isNumericalType(type_for_object2) )
			{
				// both are numerical type, but not exact the same
				
				LOGGER.debug("Comparing two different numberical type:"+Tuple.getTypeString(type_for_object1)+" vs "+Tuple.getTypeString(type_for_object2));
				
				_compare_result = compare( ((Number)v1).doubleValue(), ((Number)v2).doubleValue());
				
			}
			else if ( Tuple.isDateType(type_for_object1) && Tuple.isDateType(type_for_object2) )
			{
				// both are date type, but not exact the same
				LOGGER.debug("Comparing two different date type:"+Tuple.getTypeString(type_for_object1)+" vs "+Tuple.getTypeString(type_for_object2));
				
				if( type_for_object1==Tuple.TIME_TYPE ||  type_for_object2==Tuple.TIME_TYPE )
				{
					// cannot compare java.sql.Time type with java.sql.Date or java.sql.Timestamp
					throw new IllegalArgumentException("Cannot compare two columns with different types, column1 type:"+Tuple.getTypeString(Tuple.getType(object1))+", colum2 type:"+Tuple.getTypeString(Tuple.getType(object2)));
				}
				
				// one of them is java.sql.Date, the other one is java.sql.Timestamp,
				java.util.Date d1 = (java.util.Date)v1;
				java.util.Date d2 = (java.util.Date)v2;
				_compare_result = d1.compareTo(d2);
			}
			
			else
			{
				throw new IllegalArgumentException("Cannot compare two columns with different types, column1 type:"+Tuple.getTypeString(Tuple.getType(object1))+", colum2 type:"+Tuple.getTypeString(Tuple.getType(object2)));
			}
		}
		
		// comparing complete
		if (_compare_result==Integer.MAX_VALUE )
			throw new IllegalArgumentException();
		else
			return _compare_result;
	}
	
	
	
	
	private TreeMap<String, Integer> getIdxMapping(Sorter[] sorters)
	{
		// the ordering of the values of the columns in k1 and k2 
		// are sorted by the column name's alphabetic ordering, 
		// see {@link Tuple#write}.  We might not have the schema
		// of the tuple here, so we need to build the index according
		// to the selected columns from the sorters in alphabetic order,
		// then we can get the value directly using index.	
		if( _IDX_MAPPING==null )
		{
			_IDX_MAPPING = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);
			
			List<String> columnNames = new ArrayList<String>();
			for( Sorter aSorter:sorters )
			{
				columnNames.add(aSorter.getColumn().toLowerCase());
			}
			Collections.sort(columnNames);
			for(int i=0;i<columnNames.size();i++)
			{
				_IDX_MAPPING.put(columnNames.get(i), i);
			}			
		}
		return _IDX_MAPPING;
	}
	
	
	private Object getValue(Tuple t, String columnName, boolean forceNumeric, Sorter[] sorters)
	{
		Object v = null;
		if( t.getSchema().length==0 )
		{
			// t is de-serialized from bytes, and schema
			// has not set, used the idxMapping to get the
			// value
			int columnIdx = this.getIdxMapping(sorters).get(columnName);
			if( forceNumeric )
				v = t.getDouble(columnIdx, Double.NaN);
			else
				v = t.get(columnIdx); // use its original value
		}
		else
		{
			// this tuple has schema, use the column name
			// directly to get the value
			if( forceNumeric )
				v = t.getDouble(columnName, Double.NaN);
			else
				v = t.get(columnName);
		}		
		return v;
	}
	
	
	
	private static int compare(double v1, double v2)
	{
		return Double.compare(v1, v2);
	}
	
	private static int compare(String v1, String v2)
	{
		return v1.compareTo(v2);
	}
	
	private static int compare(TreeMap<String, String> m1, TreeMap<String, String> m2)
	{
		int _COMPARE_RESULT = Integer.MAX_VALUE;
		
		int m1_size = m1.size();
		int m2_size = m2.size();
		
		if( m1_size==0 ||m2_size==0 )
		{
			if( m1_size==m2_size )
				return 0;
			else if( m1_size!=0 )
				return 1;
			else
				return -1;
		}
		
		Iterator<String> k1_it = m1.keySet().iterator();
		Iterator<String> k2_it= m2.keySet().iterator();
		
		boolean hasDiff = false;
		
		while( k1_it.hasNext() )
		{
			String k1 = k1_it.next();
			
			if (k2_it.hasNext() )
			{
				String k2 = k2_it.next();
				
				_COMPARE_RESULT = String.CASE_INSENSITIVE_ORDER.compare(k1, k2);
				if ( _COMPARE_RESULT==0 )
				{
					// same key, check their value
					String v1 = m1.get(k1);
					String v2 = m2.get(k2);
					
					_COMPARE_RESULT = v1.compareTo(v2);
				}
			}
			else
			{
				// m1 has more keys than m2 and m1 has the same
				// values for all the keys in m2
				_COMPARE_RESULT = 1;
			}
			
			if (_COMPARE_RESULT!=0 && _COMPARE_RESULT!=Integer.MAX_VALUE )
			{
				hasDiff = true;
				break;// has result
			}
		}
		
		if( !hasDiff )
		{
			if ( k2_it.hasNext() )
			{
				// m2 has more keys than m1, and m2 has the same
				// values for all the keys in m1
				_COMPARE_RESULT = -1;
			}
			else
			{
				// m1 and m2 are the same
				
			}
		}
		
		return _COMPARE_RESULT;
	}
	
	private static int compare(Comparable v1, Comparable v2)
	{
		return v1.compareTo(v2);
	}
	
	
	
	private static final class ObjectReader
	{
		private Object obj;
		private DataInput in;
		private boolean fromIO;
		
		private ReadFieldImpl reader;
		
		private List<Object> value = new ArrayList<Object>(1);
		
		private byte type;
		
		public ObjectReader(Object obj, byte type)
		{
			fromIO = (obj instanceof DataInput);
			this.obj = obj;
			if( fromIO )
			{
				this.in = (DataInput)obj;
				this.reader = new ReadFieldImpl(value, this.in, null);
				this.type = type;
			}
			else
				this.type = Tuple.getType(this.obj);
		}
		
		public Object getValue()
			throws IOException
		{
			Object result = null;
			if( this.fromIO )
			{
				this.reader.handle(type);
				result = this.value.get(0);
			}
			else
				result = this.obj;
			
			if( result instanceof ResultWrapper )
			{
				result = ((ResultWrapper)result).getCombinedResult();
			}
			
			return result;
		}
	}
}
