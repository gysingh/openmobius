package com.ebay.erl.mobius.core.mapred;

import java.io.IOException;
import java.util.IllegalFormatException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import com.ebay.erl.mobius.core.builder.TSVDatasetBuilder;
import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.util.SerializableUtil;

/**
 * A mapper that parses the values (in {@link Text} format)
 * into {@link Tuple} elements.
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
 */
@SuppressWarnings("deprecation")
public class TSVMapper extends AbstractMobiusMapper<LongWritable, Text> 
{
	/**
	 * delimiter of the underline file, it is
	 * tab by default, and can be changed using
	 * {@link TSVDatasetBuilder#setDelimiter(String)}
	 * 
	 */
	protected String delimiter;
	
	
	/**
	 * schema of this TSV dataset
	 */
	protected String[] schema;
	
	/**
	 * {@inheritDoc}
	 * <p>
	 * 
	 * setup the {@link #delimiter} and {@link #schema}
	 * from Hadoop configuration.
	 * <p>
	 * 
	 * {@link #delimiter} is set in {@link TSVDatasetBuilder#setDelimiter(String)}
	 * and {@link #schema} is set in {@link TSVDatasetBuilder#newInstance(com.ebay.erl.mobius.core.MobiusJob, String, String[])},
	 * both are stored in Hadoop configuration and Mobius
	 * retrieve the values from here.
	 * 
	 */
	@Override
	public void configure(JobConf conf)
	{
		super.configure (conf);
		
		///////////////////////////
		// setup delimiter
		///////////////////////////
		try
		{
			this.delimiter = this.conf.get(this.getDatasetID()+".delimiter", "\t");
			if ( !this.delimiter.equals("\t") )
			{
				// if the delimiter is not tab, by default, Mobius will encoded the delimiter,
				// attempting to decode it.
				this.delimiter = (String)SerializableUtil.deserializeFromBase64(delimiter, null);
			}
		}
		catch(IOException e)
		{
			throw new RuntimeException("Unable to decode the delimiter["+this.conf.get(this.getDatasetID()+".delimiter")+"]" +
					" for dataset ["+this.getDatasetID()+"] using Base64.");
		}
		
		
		///////////////////////////
		// setup schema
		///////////////////////////
		if( this.conf.get(this.getDatasetID()+".schema", "").trim().isEmpty() )
			throw new IllegalArgumentException("Please define schema for dataset:"+this.getDatasetID ());
				
		this.schema = this.conf.get(this.getDatasetID()+".schema", "").split(",");
	}

	/**
	 * Parse the <code>invalue</code> into {@link Tuple}
	 * with the schema given in {@link TSVDatasetBuilder}.
	 * <p>
	 * 
	 * <code>invalue</code> is delimited by the {@link #delimiter},
	 * and stored in a {@link Tuple} all in java.lang.String type, 
	 * then the {@link #schema} will be assigned to the tuple.
	 * <p>
	 * 
	 * If the length of the value array (the delimited result
	 * of <code>invalue</code>) is lesser than the length
	 * of {@link #schema}, the <code>null</code> will be placed
	 * for those columns which don't have value.
	 * <p>
	 * 
	 * If the length of the value array is longer that
	 * the {@link #schema}, then "IDX_$i" will be used
	 * for those unnamed values, where $i start from
	 * the length of {@link #schema}.
	 */
	@Override
	public Tuple parse(LongWritable inkey, Text invalue)
			throws IllegalFormatException, IOException 
	{		
		Tuple tuple		= Tuple.valueOf(invalue, this.schema, this.delimiter);		
		return tuple;
	}
}
