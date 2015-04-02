package com.ebay.erl.mobius.core;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.util.SerializableUtil;
import com.ebay.erl.mobius.util.Util;

/**
 * Specify the "group-by" columns in a grouping
 * job.
 * <p>
 * 
 * This class cannot be initialized directly
 * by users.  Use {@link MobiusJob#group(Dataset)}
 * to get an instance of this class and start a
 * group-by job.
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
@SuppressWarnings("deprecation")
public class GroupByConfigure 
{
	private Configuration jobConf;
	
	private Dataset dataset;
	
	GroupByConfigure(Configuration jobConf, Dataset dataset)
	{
		if( dataset==null )
		{
			throw new IllegalArgumentException("Group-by must be performed with one dataset.");
		}
		
		this.jobConf	= jobConf;
		this.dataset	= dataset;
	}
	
	
	/**
	 * Specify the columns to be grouped by.
	 * <p>
	 * 
	 * <code>columns</code> must all in the 
	 * participated {@link Dataset}, the 
	 * one specified in {@link MobiusJob#group(Dataset)}.
	 */
	public Persistable by(String... columns)
		throws IOException
	{
		if( columns==null || columns.length==0 )
		{
			throw new IllegalArgumentException("Please specify the columns to group by.");
		}
		
		Column[] projections = new Column[columns.length];
		for( int i=0;i<columns.length;i++)
		{
			projections[i] = new Column(this.dataset, columns[i]);
		}
		
		// check if the specified columns are in the selected
		// dataset or not.
		JobSetup.validateColumns(this.dataset, projections);
		
		Byte datasetID = 0;
		
		// validation complete, set the key column
		Configuration aJobConf		= this.dataset.createJobConf(datasetID);		
		this.jobConf				= Util.merge(this.jobConf, aJobConf);
		this.jobConf.set("mapred.job.name", "Group "+this.dataset.getName()+" by "+Arrays.toString(columns));
		this.jobConf.set(ConfigureConstants.MAPPER_CLASS, this.dataset.getMapper().getCanonicalName());
		
		String joinKeyPropertyName	= datasetID+".key.columns";
		
		for( Column aColumn:projections )
		{
			if( this.jobConf.get(joinKeyPropertyName, "").isEmpty() )
			{
				this.jobConf.set (joinKeyPropertyName, aColumn.getInputColumnName ());
			}
			else
			{
				this.jobConf.set (joinKeyPropertyName, this.jobConf.get (joinKeyPropertyName) + "," + aColumn.getInputColumnName ());
			}
		}
		this.jobConf.set(ConfigureConstants.ALL_GROUP_KEY_COLUMNS, SerializableUtil.serializeToBase64(projections));
		return new Persistable(new JobConf(this.jobConf), this.dataset);
	}
}
