package com.ebay.erl.mobius.core.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;

/**
 * An array to hold a list of elements in a single column.  Only
 * the elements that within the {@link Tuple}'s supported types
 * can be added into an array.
 * 
 * <p>
 * This product is licensed under the Apache License,  Version 2.0, 
 * available at http://www.apache.org/licenses/LICENSE-2.0.
 * 
 * This product contains portions derived from Apache hadoop which is 
 * licensed under the Apache License, Version 2.0, available at 
 * http://hadoop.apache.org.
 * 
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Jack Shen, Gyanit Singh, Neel Sundaresan
 *
 */
public class Array implements WritableComparable<Array>, Configurable, Iterable<Object>{

	protected Configuration conf; 
	
	private List<Object> elements = new ArrayList<Object>();
	
	private TupleColumnComparator comparator = new TupleColumnComparator();
	
	private String delimiter = ";";
	
	public Array()
	{
		this(";");
	}
	
	public Array(String delimiter)
	{
		if( delimiter==null || delimiter.length()==0 )
			throw new IllegalArgumentException("Delimiter cannot be null nor empty.");
		
		this.delimiter = delimiter;
	}
	
	public void add(Object obj)
	{
		// make sure the object is a type
		// supported by Tuple.
		Tuple.getType(obj);
		
		this.elements.add(obj);
	}
	
	/**
	 * Contact the values with specified delimiter (default is
	 * semicolon) and return the string representation.
	 */
	@Override
	public String toString()
	{
		StringBuffer str = new StringBuffer();
		Iterator<Object> it = this.elements.iterator();
		while( it.hasNext() )
		{
			str.append(it.next().toString());
			if( it.hasNext() )
				str.append(this.delimiter);
		}
		return str.toString();
	}
	
	
	@Override
	public void readFields(DataInput in)
		throws IOException 
	{	
		int elements_nbrs = in.readInt();
			
		ReadFieldImpl read_impl = new ReadFieldImpl(this.elements, in, this.conf);
			
		for( int i=0;i<elements_nbrs;i++ )
		{
			byte type = in.readByte();
			read_impl.handle(type);
		}
	}

	@Override
	public void write(DataOutput out)
		throws IOException 
	{
		out.writeInt(this.size());
		WriteImpl writeImpl = new WriteImpl(out);
		for( Object anObject:this.elements )
		{
			byte type = Tuple.getType(anObject);
			out.write(type);
			
			writeImpl.setValue(anObject);
			writeImpl.handle(type);
		}
	}

	@Override
	public int compareTo(Array another) 
	{
		int bound = Math.min(this.size(), another.size());
		for( int i=0;i<bound;i++ )
		{
			Object o1 = this.elements.get(i);
			Object o2 = another.elements.get(i);
			
			try {
				int diff = comparator.compare(o1, o2, getConf());
				if( diff!=0 )
					return diff;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		// all the same, use number of columns 
		return this.size()-another.size();
	}

	@Override
	public Configuration getConf() 
	{
		return this.conf;
	}

	@Override
	public void setConf(Configuration conf)
	{
		this.conf = conf;
	}

	@Override
	public Iterator<Object> iterator() 
	{
		return this.elements.iterator();
	}
	
	public int size()
	{
		return this.elements.size();
	}
	
	public void clear(){
		this.elements.clear();
	}

}
