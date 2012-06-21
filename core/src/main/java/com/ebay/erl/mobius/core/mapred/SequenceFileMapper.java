package com.ebay.erl.mobius.core.mapred;

import java.io.IOException;

import com.ebay.erl.mobius.core.builder.SeqFileDatasetBuilder;
import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.util.Util;

/**
 * The base class for parsing a SequenceFile into Tuple,
 * sub-class needs to override {@link #parseKey(Object)}
 * and {@link #parseValue(Object)}.
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
 * 
 * @param <K> key in a sequence file record.
 * @param <V> value in a sequence file record.
 */
public abstract class SequenceFileMapper<K, V> extends AbstractMobiusMapper<K, V> 
{
	private String[] schema;
	
	
	/**
	 * Parse the <code>inkey</code> and <code>invalue</code>
	 * and merge them into one {@link Tuple}, and set the
	 * schema using {@link #getSchema()}.
	 * <p>
	 * 
	 * There are X number of columns from <code>inkey</code>, 
	 * and Y numbers of columns from <code>invalue</code>, so
	 * the returned Tuple contains X+Y number of columns.  X+Y
	 * <b>must</b> be equals to the size of {@link #getSchema()}. 
	 * <p>
	 * 
	 * The returned Tuple will be a row of this dataset.
	 */
	@Override
	public abstract Tuple parse(K inkey, V invalue)	
		throws IllegalArgumentException, IOException;
	
	
	
	
	/**
	 * The schema for the Tuple returned by {@linkplain #parse(Object, Object)}.
	 * <p>
	 * 
	 * This schema is set when the user built the dataset using 
	 * {@link SeqFileDatasetBuilder}.
	 * 
	 */
	protected String[] getSchema()
	{
		if( this.schema==null||this.schema.length==0 )
		{
			///////////////////////////
			// setup schema
			///////////////////////////			
			this.schema = this.conf.getStrings(this.getDatasetID()+".schema", Util.ZERO_SIZE_STRING_ARRAY);			
			if( this.schema.length==0 )
			{
				throw new IllegalArgumentException("Please setup schema for "+this.getDatasetID()+".");
			}
		}
		return this.schema;
	}
}
