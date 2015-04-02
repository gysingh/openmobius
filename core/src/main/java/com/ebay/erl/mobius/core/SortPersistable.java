package com.ebay.erl.mobius.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;

import com.ebay.erl.mobius.core.builder.AbstractDatasetBuilder;
import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.builder.DatasetBuildersFactory;
import com.ebay.erl.mobius.core.datajoin.DataJoinKey;
import com.ebay.erl.mobius.core.datajoin.DataJoinValue;
import com.ebay.erl.mobius.core.function.base.Projectable;
import com.ebay.erl.mobius.core.mapred.TotalSortReducer;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.util.Util;

/**
 * Specifies the output path and format of a 
 * sort job.
 * <p>
 * 
 * See {@link MobiusJob#sort(Dataset)} for information
 * on creating a sort job.
 * 
 * 
 * 
 * This product is licensed under the Apache License,  Version 2.0, 
 * available at http://www.apache.org/licenses/LICENSE-2.0.
 * 
 * This product contains portions derived from Apache hadoop which is 
 * licensed under the Apache License, Version 2.0, available at 
 * http://hadoop.apache.org.
 * 
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Jack Shen, Gyanit Singh, Neel Sundaresan
 */
@SuppressWarnings({"deprecation", "unchecked"})
public class SortPersistable 
{	
	private JobConf jobConf;
	
	private Configuration userDefinedConf;
	
	private Collection<Column> projections;
	
	SortPersistable(JobConf conf, Collection<Column> projections)
	{
		this.jobConf = conf;
		this.projections = projections;
	}
	
	/**
	 * set a configuration property to this job's 
	 * configuration.
	 */
	public SortPersistable setConf(String name, String value)
	{
		if( userDefinedConf==null )
		{
			this.userDefinedConf = new Configuration(false);
		}
		this.userDefinedConf.set(name, value);
		return this;
	}
	
	/**
	 * Save the sort result to the given <code>output</code>.
	 * <p>
	 * 
	 * The returned {@link Dataset} represents the sorted result,
	 * it can be used to do further analysis.
	 */
	public Dataset save(MobiusJob job, Path output)
		throws IOException
	{
		return this.save(job, output, TextOutputFormat.class);
	}
	
	
	/**
	 * Save the sort result to the given <code>output</code> with
	 * the specified <code>outputFormat</code>.
	 * <p>
	 * 
	 * The returned {@link Dataset} represents the sorted result,
	 * it can be used to do further analysis.
	 */
	public Dataset save(MobiusJob job, Path output, Class<? extends FileOutputFormat> outputFormat)
		throws IOException
	{
		// SETUP JOB
		if( this.userDefinedConf!=null )
		{
			this.jobConf = new JobConf(Util.merge(this.jobConf, this.userDefinedConf));
		}
		this.jobConf.setJarByClass(job.getClass());
		this.jobConf.setMapOutputKeyClass(DataJoinKey.class);
		this.jobConf.setMapOutputValueClass(DataJoinValue.class);
		this.jobConf.setPartitionerClass (TotalOrderPartitioner.class);
		this.jobConf.setOutputKeyComparatorClass (DataJoinKey.class);
		this.jobConf.setReducerClass(TotalSortReducer.class);		
		
		JobSetup.setupOutputs(this.jobConf, output, outputFormat);
		
		job.addToExecQueue(this.jobConf);
		
		
		AbstractDatasetBuilder builder = DatasetBuildersFactory.getInstance(job).getBuilder(outputFormat, "Dataset_"+output.getName());
		
		// form the output column from the projections
		List<String> outputColumns = new ArrayList<String>();
		for( Projectable func:projections )
		{
			String[] aProjectOutputs = func.getOutputSchema();
			for(String anOutputName:aProjectOutputs)
			{
				outputColumns.add(anOutputName);
			}
		}
		
		return builder.buildFromPreviousJob(jobConf, outputFormat, outputColumns.toArray(new String[0]));
	}
}
