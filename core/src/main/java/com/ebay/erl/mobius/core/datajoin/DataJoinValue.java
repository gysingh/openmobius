package com.ebay.erl.mobius.core.datajoin;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

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
public class DataJoinValue extends Tuple{
	
	public static String DATASET_ID		= "00_MOBIUS_DATASETID";
	public static String ACTUAL_VALUE	= "01_MOBIUS_VALUE";

	// to be called by 
	// org.apache.hadoop.io.serializer.WritableSerialization$WritableDeserializer.deserialize	
	public DataJoinValue(){}
	
	public DataJoinValue(Byte datasetID, WritableComparable<?> value) 
	{
		set(datasetID, value);
	}
	
	public void set(Byte datasetID, WritableComparable<?> value)
	{
		this.put(DATASET_ID, datasetID.byteValue());
		this.put(ACTUAL_VALUE, value);
	}
	
	public Byte getDatasetID() 
	{
		return this.getByte(DATASET_ID);
	}

	public WritableComparable<?> getValue() 
	{
		return (WritableComparable<?>)this.get(ACTUAL_VALUE);
	}
	
	@Override
	public void readFields(DataInput in)
		throws IOException 
	{	
		super.readFields(in);
		
		// ordering matters
		this.setSchema(new String[]{DATASET_ID, ACTUAL_VALUE});
	}


}
