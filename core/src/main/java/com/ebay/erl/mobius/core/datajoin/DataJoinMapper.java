package com.ebay.erl.mobius.core.datajoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

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
 * @param <IK>
 * @param <IV>
 * @param <OK>
 * @param <OV>
 */
@SuppressWarnings({"deprecation", "unchecked"})
public abstract class DataJoinMapper<IK, IV, OK extends WritableComparable<?>, OV extends WritableComparable<?>>
		extends MapReduceBase implements Mapper<IK, IV, OK, OV> {

	protected Configuration conf;
	
	protected boolean hasReducer;
	
	@Override
	public void configure(JobConf job) 
	{
		super.configure(job);
		this.conf = job;
		this.hasReducer = this.conf.getInt("mapred.reduce.tasks", 1)!=0;
	}

	@Override
	public void map(IK key, IV value, OutputCollector<OK, OV> output, Reporter reporter)
		throws IOException 
	{
		if( hasReducer )
		{
			// use mobius data join
			OutputCollector<OK, OV> joinOutput = new DataJoinOutputCollector (this, output, this.conf);
			joinmap (key, value, joinOutput, reporter);
		}
		else
		{
			joinmap (key, value, output, reporter);
		}
	}

	public abstract void joinmap(IK key, IV value, OutputCollector<OK, OV> output, Reporter reporter)
		throws IOException;	

	public abstract String getDatasetID();

	public WritableComparable<?> extractSortValueKeyword(OV value) 
	{
		return null;
	}

	public Class<?> getSortValueComparator() 
	{
		return null;
	}
}
