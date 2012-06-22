package com.ebay.erl.mobius.core.builder;

import java.io.IOException;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import com.ebay.erl.mobius.core.MobiusJob;
import com.ebay.erl.mobius.core.mapred.AbstractMobiusMapper;
import com.ebay.erl.mobius.core.mapred.TSVMapper;
import com.ebay.erl.mobius.util.SerializableUtil;

/**
 * Represents a dataset backed with the Hadoop text format.
 * 
 * 
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
 * @see TSVDatasetBuilder
 * @see TSVMapper
 */
@SuppressWarnings({"deprecation", "unchecked"})
public class TSVDataset extends Dataset
{
	private static final long serialVersionUID = -4596104689783253807L;
	private String delimiter = "\t";
	
	/**
	 * Create an instance of dataset with {@link TextInputFormat}
	 * and {@link TSVMapper} as default.
	 * <p>
	 * 
	 * User should not create instance of this class
	 * directly through constructor, but to use 
	 * {@link TSVDatasetBuilder} to build one.
	 * 
	 * @param job a Mobius job contains an analysis flow.
	 * @param name the name of this dataset.
	 */
	protected TSVDataset(MobiusJob job, String name)
	{
		super(job, name);
		this.input_format	= TextInputFormat.class;
		this.mapper			= TSVMapper.class;
	}
	
	TSVDataset setDelimiter(String delimiter)
	{
		this.delimiter = delimiter;
		return this;
	}
	
	
	
	/**
	 * Always throw {@link UnsupportedOperationException} as one cannot change 
	 * the input format, the input format is fixed to {@link TextInputFormat}.
	 */
	@Override
	protected void setInputFormat(Class<? extends InputFormat> input_format)
	{
		throw new UnsupportedOperationException("Cannot set different inputformat to TSV dataset");
	}
	
	
	
	/**
	 * only accept a mapper which is the subclass of {@link TSVMapper}
	 */
	@Override
	protected void setMapper(Class<? extends AbstractMobiusMapper> mapper)
	{
		if( TSVMapper.class.isAssignableFrom(mapper) )
		{
			// mapper extends TSVMapper
			this.mapper = mapper;
		}
		else
		{
			throw new IllegalArgumentException("Only class which extends "+TSVMapper.class.getCanonicalName()+" is accepted.");
		}
	}
	
	@Override
	public JobConf createJobConf(byte jobSequenceNumber)
		throws IOException
	{
		JobConf conf = super.createJobConf(jobSequenceNumber);
		if( !this.delimiter.equals("\t") )
		{
			conf.set(this.getID()+".delimiter", SerializableUtil.serializeToBase64(delimiter));
		}
		return conf;
	}
}
