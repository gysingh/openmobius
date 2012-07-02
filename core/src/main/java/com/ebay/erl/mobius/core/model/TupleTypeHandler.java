package com.ebay.erl.mobius.core.model;

import java.io.IOException;

/**
 * Base class for handling all supported types
 * from {@link Tuple}.
 * <p>
 * 
 * This class is used by the Mobius engine.
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
public abstract class TupleTypeHandler <RETURN> 
{
	protected byte type;
	
	public RETURN handle(byte type)	
		throws IOException
	{
		this.type = type;
		
		switch(this.type)
		{
			case Tuple.BYTE_TYPE:
				return on_byte();
			case Tuple.SHORT_TYPE:
				return on_short();
			case Tuple.INTEGER_TYPE:
				return on_integer();
			case Tuple.LONG_TYPE:
				return on_long();
			case Tuple.FLOAT_TYPE:
				return on_float();
			case Tuple.DOUBLE_TYPE:
				return on_double();
			case Tuple.STRING_TYPE:
				return on_string();
			case Tuple.DATE_TYPE:
				return on_date();
			case Tuple.TIMESTAMP_TYPE:
				return on_timestamp();
			case Tuple.TIME_TYPE:
				return on_time();
			case Tuple.BOOLEAN_TYPE:
				return on_boolean();
			case Tuple.STRING_MAP_TYPE:
				return on_string_map();
			case Tuple.RESULT_WRAPPER_TYPE:
				return on_result_wrapper();
			case Tuple.WRITABLE_TYPE:
				return on_writable();
			case Tuple.SERIALIZABLE_TYPE:
				return on_serializable();
			case Tuple.NULL_TYPE:
				return on_null();
			case Tuple.BYTE_ARRAY_TYPE:
				return on_byte_array();
			case Tuple.NULL_WRITABLE_TYPE:
				return on_null_writable();
			case Tuple.TUPLE_TYPE:
				return on_tuple();
			case Tuple.ARRAY_TYPE:
				return on_array();
			default:
				return on_default();
		}
	}
	
	protected abstract RETURN on_byte() throws IOException;
	
	protected abstract RETURN on_short() throws IOException;
	
	protected abstract RETURN on_integer() throws IOException;
	
	protected abstract RETURN on_long() throws IOException;
	
	protected abstract RETURN on_float() throws IOException;
	
	protected abstract RETURN on_double() throws IOException;
	
	protected abstract RETURN on_string() throws IOException;
	
	protected abstract RETURN on_date() throws IOException;
	
	protected abstract RETURN on_timestamp() throws IOException;
	
	protected abstract RETURN on_time() throws IOException;
	
	protected abstract RETURN on_boolean() throws IOException;
	
	protected abstract RETURN on_string_map() throws IOException;
	
	protected abstract RETURN on_writable() throws IOException;
	
	protected abstract RETURN on_serializable() throws IOException;
	
	protected abstract RETURN on_null() throws IOException;
	
	protected abstract RETURN on_default() throws IOException;
	
	protected abstract RETURN on_byte_array() throws IOException;
	
	protected abstract RETURN on_tuple() throws IOException;
	
	protected abstract RETURN on_null_writable() throws IOException;
	
	protected abstract RETURN on_result_wrapper() throws IOException;
	
	protected abstract RETURN on_array() throws IOException;
}
