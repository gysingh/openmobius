package com.ebay.erl.mobius.core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.util.SerializableUtil;
import com.ebay.erl.mobius.util.Util;

/**
 * Specifies the columns to be stored of a sort job.
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
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Neel Sundaresan
 */
@SuppressWarnings("deprecation")
public class SortProjectionConfigure 
{
	private JobConf conf;
	
	/**
	 * the {@link Dataset} to be sorted.
	 */
	private Dataset aDataset;
	
	
	SortProjectionConfigure(Configuration conf, Dataset aDataset)
		throws IOException
	{
		Configuration aJobConf	= aDataset.createJobConf(0);
		this.conf				= new JobConf(Util.merge(conf, aJobConf));
		this.conf.set(ConfigureConstants.IS_SORT_JOB, "true");
		this.conf.set(ConfigureConstants.MAPPER_CLASS, aDataset.getMapper().getCanonicalName());
		this.aDataset = aDataset;
	}
	
	/**
	 * Select the columns to be projected (saved in disk) for 
	 * this sort job.
	 */
	public SortOrderByConfigure select(String... columns)
		throws IOException
	{
		Column[] projections = new Column[columns.length];
		for( int i=0;i<columns.length;i++ )
		{
			projections[i] = new Column(this.aDataset, columns[i]);
		}
		
		JobSetup.validateColumns(aDataset, projections);
		JobSetup.setupProjections(this.conf, aDataset, 0, projections);
		JobSetup.setupInputs(this.conf, aDataset, 0);
		
		String id = aDataset.getDatasetID(0);
		this.conf.set(ConfigureConstants.ALL_DATASET_IDS, id);
		
		// specify the columns that reducer need to project
		this.conf.set(ConfigureConstants.PROJECTION_COLUMNS, SerializableUtil.serializeToBase64(projections));
		
		return new SortOrderByConfigure(this.conf, this.aDataset, projections);
	}
}
