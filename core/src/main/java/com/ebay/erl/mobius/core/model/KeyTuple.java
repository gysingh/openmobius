package com.ebay.erl.mobius.core.model;

import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * Same as {@link Tuple}, only the ordering is the same
 * as the insertion ordering.  This class is used by Mobius
 * to emit join key. 
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
public class KeyTuple extends Tuple
{	
	public KeyTuple()
	{
		this.namesToIdxMapping = new LinkedHashMap<String, Integer>();
	}
	
	@Override
	public void setSchema(String[] schema)
	{
		this.namesToIdxMapping.clear();
		int idx = 0;
		for( String aName:schema )
		{
			this.namesToIdxMapping.put(lowerCase(aName), idx++);
		}
	}
	
	
	@Override
	public void write(DataOutput out) 
		throws IOException 
	{	
		// write the size of the column of this tuple
		out.writeInt(this.values.size());
		
		if( this.values.size()!=this.namesToIdxMapping.size() )
		{
			StringBuffer sb = new StringBuffer();
			for( Object v:values)
				sb.append(v.toString()).append(",");
			throw new IllegalArgumentException(this.getClass().getCanonicalName()+", the length of values and schmea is not the same, " +
					"very likely the schema of this tuple has not been set yet, please set it using Tuple#setSchema(String[])." +
					" Values:["+sb.toString()+"] schema:"+this.namesToIdxMapping.keySet());
		}
		
		WriteImpl writeImpl = new WriteImpl(out);
		
		for ( String aColumnName : this.namesToIdxMapping.keySet() )
		{				
			Object value = this.values.get(this.namesToIdxMapping.get(aColumnName));
			byte type = getType(value);
			out.write(type);
			
			writeImpl.setValue(value);
			writeImpl.handle(type);
		}
	}
}
