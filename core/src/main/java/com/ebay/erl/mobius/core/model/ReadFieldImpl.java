package com.ebay.erl.mobius.core.model;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

import com.ebay.erl.mobius.core.collection.CaseInsensitiveTreeMap;
import com.ebay.erl.mobius.util.SerializableUtil;
import com.ebay.erl.mobius.util.Util;

/**
 * Used by the Mobius engine to de-serialize
 * {@link Tuple} from binary.
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
public class ReadFieldImpl extends TupleTypeHandler<Void> 
{

	private List<Object> values;
	private DataInput in;
	private Configuration conf;
	
	public ReadFieldImpl(List<Object> values, DataInput in, Configuration conf) 
	{	
		this.values = values;
		this.in = in;
		this.conf = conf;
	}

	@Override
	protected Void on_boolean() 
		throws IOException
	{	
		this.values.add(in.readBoolean());
		return null;
	}

	@Override
	protected Void on_byte() 
		throws IOException
	{
		this.values.add(in.readByte());	
		return null;
	}

	@Override
	protected Void on_timestamp() 
		throws IOException
	{
		Timestamp ts = new Timestamp(in.readLong());		
		this.values.add(ts);
		return null;
	}

	@Override
	protected Void on_date() 
		throws IOException
	{
		this.values.add(new java.sql.Date(in.readLong()));
		return null;
	}

	@Override
	protected Void on_default() 
		throws IOException
	{
		throw new IllegalArgumentException("Unsupported type ["+String.format("0x%02X", type)+"]");
	}

	@Override
	protected Void on_double() 
		throws IOException
	{
		this.values.add(in.readDouble());
		return null;
	}

	@Override
	protected Void on_float()
		throws IOException
	{
		this.values.add(in.readFloat());
		return null;
	}

	@Override
	protected Void on_integer()
		throws IOException
	{
		this.values.add(in.readInt());
		return null;
	}
	
	@Override
	protected Void on_long()
		throws IOException
	{
		this.values.add(in.readLong());
		return null;
	}

	@Override
	protected Void on_null()
		throws IOException
	{
		this.values.add(null);
		return null;
	}

	@Override
	protected Void on_serializable()
		throws IOException
	{
		Serializable obj = (Serializable)SerializableUtil.deserializeFromBase64(in.readUTF(), this.conf);
		this.values.add(obj);
		return null;
	}

	@Override
	protected Void on_short()
		throws IOException
	{
		this.values.add(in.readShort());
		return null;
	}

	@Override
	protected Void on_string()
		throws IOException
	{
		this.values.add(Text.readString(in));
		return null;
	}

	@Override
	protected Void on_string_map()
		throws IOException
	{
		int map_size = in.readInt();
		CaseInsensitiveTreeMap map = new CaseInsensitiveTreeMap();		
		for( int j=0;j<map_size;j++ )
		{
			String k = Text.readString(in);
			String v = Text.readString(in);
			map.put(k, v);
		}
		this.values.add(map);
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Void on_writable()
		throws IOException
	{
		WritableComparable writable = (WritableComparable)ReflectionUtils.newInstance(Util.getClass(in.readUTF()), this.conf);
		writable.readFields(in);
		this.values.add(writable);
		
		return null;
	}
	
	@Override
	protected Void on_byte_array()
		throws IOException
	{
		int length = in.readInt();
		byte[] array = new byte[length];
		in.readFully(array);
		this.values.add(array);
		return null;
	}

	@Override
	protected Void on_null_writable() throws IOException {
		this.values.add(NullWritable.get());
		return null;
	}

	@Override
	protected Void on_tuple() throws IOException {
		Tuple newTuple = new Tuple();
		newTuple.readFields(in);
		this.values.add(newTuple);
		return null;
	}

	@Override
	protected Void on_time() throws IOException {
		Time time = new Time(in.readLong());
		this.values.add(time);
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Void on_result_wrapper() throws IOException {
		ResultWrapper<?> wrapper = new ResultWrapper();
		wrapper.readFields(in);
		this.values.add(wrapper);
		return null;
	}

	@Override
	protected Void on_array() throws IOException {
		Array array = new Array();
		array.readFields(in);
		this.values.add(array);
		return null;
	}

}
