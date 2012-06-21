package com.ebay.erl.mobius.core.datajoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.ReflectionUtils;

import com.ebay.erl.mobius.core.ConfigureConstants;
import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.core.model.TupleColumnComparator;
import com.ebay.erl.mobius.core.sort.Sorter;
import com.ebay.erl.mobius.util.SerializableUtil;
import com.ebay.erl.mobius.util.Util;
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
@SuppressWarnings("unchecked")
public class DataJoinKey extends Tuple {
	
	// add 2 digit prefix to ensure when deserialize it from byte arrays,
	// we preserve the order, because when {@link Tuple} serialize itself,
	// it iterates column names (in natural order) one by one to serialize
	// the values of the columns.
	public static String KEY_FIELDNAME				= "00_MOBIUS_KEY"; 
	public static String DATASET_ID_FIELDNAME		= "01_MOBIUS_DATASETID";	
	//public static String SORT_KEYWORD_FIELDNAME		= "02_MOBIUS_SORT_KEYWORD";
	//public static String SORT_COMPARATOR_FIELDNAME	= "03_MOBIUS_SORT_COMPARATOR";
	
	// to be called by Hadoop on org.apache.hadoop.mapred.JobConf.getOutputKeyComparator
	public DataJoinKey(){}

	public DataJoinKey(String datasetID, WritableComparable<?> key) 
	{
		set(datasetID, key, null, null);
	}
	
	public DataJoinKey(Text datasetID, WritableComparable<?> key)
	{
		set(datasetID.toString(), key, null, null);
	}
	
	public DataJoinKey(Text datasetID, WritableComparable<?> key, WritableComparable<?> sortKeyword, Class<?> sortComparator)
	{
		set(datasetID.toString(), key, sortKeyword, sortComparator);
	}
	
	public void set(String datasetID, WritableComparable<?> key, WritableComparable<?> sortKeyword, Class<?> sortComparator)
	{
		this.put(KEY_FIELDNAME, key);
		this.put(DATASET_ID_FIELDNAME, datasetID);		
		//this.put(SORT_KEYWORD_FIELDNAME, sortKeyword==null?NullWritable.get():sortKeyword);
		//this.put(SORT_COMPARATOR_FIELDNAME, sortComparator==null?Class.class.getName():sortComparator.getName());
	}
	
	@Override
	public void readFields(DataInput in)
		throws IOException 
	{	
		super.readFields(in);
		
		// ordering matters
		this.setSchema(new String[]{KEY_FIELDNAME, DATASET_ID_FIELDNAME/*, SORT_KEYWORD_FIELDNAME, SORT_COMPARATOR_FIELDNAME*/});
	}
	
	
	public WritableComparable getKey() 
	{
		return (WritableComparable<?>)this.get(KEY_FIELDNAME);
	}

	public String getDatasetID() 
	{
		return this.getString(DATASET_ID_FIELDNAME);
	}
	
	public WritableComparable getSortKeyword() 
	{
		//return (WritableComparable)this.get(SORT_KEYWORD_FIELDNAME);
		return null;
	}
	
	public Class getSortComparator() 
	{
		//return Util.getClass(this.getString(SORT_COMPARATOR_FIELDNAME));
		return null;
	}
	
	private static Sorter[] _SORTERS;
	
	private Sorter[] getSorter()
	{
		if( _SORTERS==null )
		{
			if( this.conf==null || this.conf.get(ConfigureConstants.SORTERS, "").isEmpty() )
			{
				_SORTERS = new Sorter[0];
			}
			else
			{
				try 
				{
					_SORTERS = (Sorter[])SerializableUtil.deserializeFromBase64(this.conf.get(ConfigureConstants.SORTERS), conf);					
					
				} catch (IOException e) 
				{
					throw new RuntimeException("Cannot deserialize sorters from :["+this.conf.get(ConfigureConstants.SORTERS)+"] using Base64 decoder.", e);
				}
			}
		}
		return _SORTERS;
	}

	@Override
	public int compareTo(Tuple other) 
	{
		WritableComparable<?> key = (WritableComparable<?>)other.get(KEY_FIELDNAME);
		int cmp = _COLUMN_COMPARATOR.compareKey(this.getKey(), key, this.getSorter(), this.conf);
		if(cmp!=0) return cmp;
		
		cmp = getDatasetID().compareTo(other.getString(DATASET_ID_FIELDNAME));
		if(cmp!=0) return cmp;
		
		return 0;
	}
	
	

	@Override
	public int compare(Tuple t1, Tuple t2) 
	{
		if(t1 instanceof DataJoinKey && t2 instanceof DataJoinKey) 
		{
			int result = t1.compareTo(t2);
			return result;
		} 
		else 
		{
			return super.compare(t1, t2);
		}
	}



	public static class Comparator extends WritableComparator
	{
		public Comparator() 
		{
			super(DataJoinKey.class);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a instanceof DataJoinKey && b instanceof DataJoinKey){
				return ((DataJoinKey)a).getKey().compareTo(((DataJoinKey)b).getKey());
			}
			return super.compare(a, b);
		}
	}
	
	private WritableComparable getKey(byte type, DataInputBuffer input)
		throws IOException
	{
		if( type==Tuple.NULL_WRITABLE_TYPE )
			return NullWritable.get();
		else if (type==Tuple.TUPLE_TYPE)
		{
			Tuple newTuple = new Tuple();
			newTuple.readFields(input);
			return newTuple;
		}
		else
		{
			WritableComparable w = (WritableComparable)ReflectionUtils.newInstance( Util.getClass(input.readUTF()), conf);
			w.readFields(input);
			return w;
		}
	}


	private final TupleColumnComparator _COLUMN_COMPARATOR = new TupleColumnComparator();
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) 
	{
		DataInputBuffer d1 = new DataInputBuffer();
		d1.reset(b1, s1, l1);
		
		DataInputBuffer d2 = new DataInputBuffer();
		d2.reset(b2, s2, l2);
		
		int _compare_result = Integer.MAX_VALUE;
		
		try
		{
			// the comparing ordering: 
			// 1. DataJoinKey#KEY_FIELDNAME
			// 2. DataJoinKey#DATASET_ID_FIELDNAME
			// 3. DataJoinKey#SORT_KEYWORD_FIELDNAME - removed
			// 4. DataJoinKey#SORT_COMPARATOR_FIELDNAME - removed
			
			// read number of columns from the two tuple,
			// but there is no need to compare the length
			// of columns, we just read the values.
			d1.readInt();
			d2.readInt();
			
			
			
			//////////////////////////////////////////////////////////
			// compare KEY, values from DataJoinKey#KEY_FIELDNAME
			// KEY represents the actual key user specified
			///////////////////////////////////////////////////////////
			byte type1 = d1.readByte();
			byte type2 = d2.readByte();
			_COLUMN_COMPARATOR.setType(type1, type2);
			
			
			// writable, check if they are Tuple or NullWritable
			if( type1==Tuple.NULL_WRITABLE_TYPE && type2==Tuple.NULL_WRITABLE_TYPE )
			{
				// consider equal, do nothing
				_compare_result = 0;
			}
			else if ( type1==Tuple.TUPLE_TYPE && type2==Tuple.TUPLE_TYPE )
			{
				// both are Tuple
				Tuple k1 = (Tuple)getKey(type1, d1);
				Tuple k2 = (Tuple)getKey(type2, d2);
				_compare_result = _COLUMN_COMPARATOR.compareKey(k1, k2, this.getSorter(), conf);
			}
			else
			{
				// DataJoinKey only support NullWritable and Tuple for the DataJoinKey#KEY_FIELDNAME
				throw new IllegalArgumentException("Cannot compare "+Tuple.getTypeString(type1)+" and "+Tuple.getTypeString(type2));
			}
			
			
			// if they are not the same, these two records should go to
			// different reducer, or different reduce iteration.
			if(_compare_result != 0) return _compare_result; 
			
			
			
			//////////////////////////////////////////////////////////////////////////
			// compare DATASET_ID, values from DataJoinKey#DATASET_ID_FIELDNAME,
			// at this point, the keys are the same, they should go to the same
			// reducer, we need to make sure the values from DATASET1 always come
			// before DATASET2, so we need to compare the DATASET_ID here.
			//////////////////////////////////////////////////////////////////////////
			try
			{
				_COLUMN_COMPARATOR.setType(d1.readByte(), d2.readByte());
				_compare_result = _COLUMN_COMPARATOR.compare(d1, d2, this.conf);
				if(_compare_result != 0) return _compare_result;
			}catch(IOException e)
			{
				byte[] b = new byte[l1];
				for( int i=0;i<l1;i++ )
				{
					b[i] = b1[s1+i];
				}
				System.err.println(Arrays.toString(b));
				System.err.println("type1:"+type1+", type2:"+type2);
				throw e;
			}
			
			return 0;
			
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		} 
	}

}

 
