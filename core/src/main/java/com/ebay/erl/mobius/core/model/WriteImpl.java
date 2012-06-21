package com.ebay.erl.mobius.core.model;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.ebay.erl.mobius.util.SerializableUtil;

/**
 * Used by the Mobius engine to serialize
 * {@link Tuple} into binary.
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
public class WriteImpl extends TupleTypeHandler<Void> 
{	
	
	private DataOutput out;
	
	private Object value;
	
	public WriteImpl(DataOutput out)
	{
		this.out = out;
	}
	
	public void setValue(Object value)
	{
		this.value = value;
	}

	@Override
	protected Void on_boolean() throws IOException {
		out.writeBoolean( ((Boolean)value) );
		return null;
	}

	@Override
	protected Void on_byte() throws IOException {
		out.writeByte( ((Byte)value) );
		return null;
	}

	@Override
	protected Void on_timestamp() throws IOException {
		out.writeLong( ((Timestamp)value).getTime() );
		return null;
	}

	@Override
	protected Void on_date() throws IOException {
		out.writeLong( ((java.sql.Date)value).getTime() );
		return null;
	}

	@Override
	protected Void on_default() throws IOException {
		throw new IllegalArgumentException(String.format("%02X", type)+" is not supported type.");
	}

	@Override
	protected Void on_double() throws IOException {
		out.writeDouble((Double)value);
		return null;
	}

	@Override
	protected Void on_float() throws IOException {
		out.writeFloat((Float)value);
		return null;
	}

	@Override
	protected Void on_integer() throws IOException {
		out.writeInt((Integer)value);
		return null;
	}

	@Override
	protected Void on_long() throws IOException {
		out.writeLong((Long)value);
		return null;
	}

	@Override
	protected Void on_null() throws IOException {
		// do nothing, as null type doesn't have value
		// and it's type has been written.
		return null;
	}

	@Override
	protected Void on_serializable() throws IOException {
		out.writeUTF(SerializableUtil.serializeToBase64((Serializable)value));
		return null;
	}

	@Override
	protected Void on_short() throws IOException {
		out.writeShort((Short)value);
		return null;
	}
	
	

	@Override
	protected Void on_string() throws IOException {
		String str 		= (String)value;
		Text.writeString(out, str);
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Void on_string_map() throws IOException {
		out.writeInt(((Map)value).size());
		for(Entry<String, String> e: (Set<Entry<String, String>>)((Map<String, String>)value).entrySet())
		{	
			Text.writeString(out, e.getKey());
			Text.writeString(out, e.getValue());
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Void on_writable() throws IOException {
		WritableComparable w = (WritableComparable)value;
		out.writeUTF(w.getClass().getCanonicalName());
		w.write(out);
		return null;
	}
	
	@Override
	protected Void on_byte_array() throws IOException
	{
		byte[] array = (byte[])value;
		out.writeInt(array.length);
		out.write(array);
		return null;
	}

	@Override
	protected Void on_null_writable() throws IOException {
		// do nothing, the type has been written in Tuple#write
		// method
		return null;
	}

	@Override
	protected Void on_tuple() throws IOException {
		Tuple w = (Tuple)value;
		w.write(out);
		return null;
	}

	@Override
	protected Void on_time() throws IOException {
		Time t = (Time)value;
		out.writeLong(t.getTime());
		return null;
	}

	@Override
	protected Void on_result_wrapper() throws IOException {
		ResultWrapper<?> wrapper = (ResultWrapper<?>)value;
		wrapper.write(out);
		return null;
	}
}
