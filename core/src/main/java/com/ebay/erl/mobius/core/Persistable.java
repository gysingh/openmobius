package com.ebay.erl.mobius.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.ebay.erl.mobius.core.builder.AbstractDatasetBuilder;
import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.builder.DatasetBuildersFactory;
import com.ebay.erl.mobius.core.criterion.TupleCriterion;
import com.ebay.erl.mobius.core.datajoin.DataJoinKey;
import com.ebay.erl.mobius.core.datajoin.DataJoinKeyPartitioner;
import com.ebay.erl.mobius.core.datajoin.DataJoinValue;
import com.ebay.erl.mobius.core.function.base.Projectable;
import com.ebay.erl.mobius.core.mapred.DefaultMobiusCombiner;
import com.ebay.erl.mobius.core.mapred.DefaultMobiusReducer;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.util.SerializableUtil;
import com.ebay.erl.mobius.util.Util;

/**
 * Sets the projections (columns to be saved on disk ) 
 * for join or group-by jobs.
 * <p>
 * 
 * The user cannot create an instance of this class
 * directly.  To get an instance of this class, use 
 * {@link JoinOnConfigure} for join type jobs, or 
 * {@link GroupByConfigure} for group-by jobs.
 * <p>
 * 
 * See {@link MobiusJob#innerJoin(Dataset...)} or
 * {@link MobiusJob#group(Dataset)} for information
 * on creating a join or group-by job.
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
@SuppressWarnings({"deprecation", "unchecked"})
public class Persistable 
{
	private Configuration userDefinedConf;
	
	private JobConf jobConf;
	
	private Dataset[] datasets;
	
	private static final Log LOGGER = LogFactory.getLog(Persistable.class);
	
	
	Persistable(Configuration jobConf, Dataset... datasets)
	{
		this.jobConf	= new JobConf(jobConf);
		this.datasets	= datasets;
	}
	
	
	
	/**
	 * set a configuration property to this job's 
	 * configuration.
	 * <p>
	 * 
	 * @param name a property name in a Hadoop job configuration.
	 * @param value the value for the property name in a Hadoop 
	 * job configuration.
	 */
	public Persistable setConf(String name, String value)
	{
		if( userDefinedConf==null )
		{
			this.userDefinedConf = new Configuration(false);
		}
		this.userDefinedConf.set(name, value);
		return this;
	}
	
	
	
	/**
	 * Specify the name of this job.
	 */
	public Persistable setJobName(String newJobName)
	{
		this.jobConf.set("mapred.job.name", newJobName);
		return this;
	}
	
	
	
	/**
	 * Specify the number of reducer of this job.
	 */
	public Persistable setReducersNumber(int reducerNumber)
	{
		if( reducerNumber<=0 )
			throw new IllegalArgumentException("number of reducer must grater than 0.");
		
		this.jobConf.setInt("mapred.reduce.tasks", reducerNumber);
		return this;
	}
	
	
	
	/**
	 * Build the dataset and store the <code>projections</code>
	 * into a temporal path (under hadoop.tmp.dir) in the format of
	 * {@link SequenceFileOutputFormat}.
	 */
	public Dataset build(MobiusJob job, Projectable... projections)
		throws IOException
	{
		return this.build(job, SequenceFileOutputFormat.class, projections);
	}
	
	
	
	/**
	 * Build the dataset and store the <code>projections</code>
	 * into a temporal path (under hadoop.tmp.dir) in the format of
	 * {@link SequenceFileOutputFormat}.
	 * <p>
	 * 
	 * Only the rows that meet the <code>criteria</code> will be 
	 * stored.  The <code>criteria</code> can only evaluate the 
	 * columns specified in the <code>projections</code>.
	 */
	public Dataset build(MobiusJob job, TupleCriterion criteria, Projectable... projections)
		throws IOException
	{
		return this.build(job, SequenceFileOutputFormat.class, criteria, projections);
	}
	
	
	
	/**
	 * Build the dataset and store the <code>projections</code>
	 * into a temporal path (under hadoop.tmp.dir) in the format of
	 * the given <code>outputFormat</code>.
	 * <p>
	 */
	public Dataset build(MobiusJob job, Class<? extends FileOutputFormat> outputFormat, Projectable... projections)
		throws IOException
	{
		return this.build(job, outputFormat, null, projections);
	}
	
	
	
	/**
	 * Build the dataset and store the <code>projections</code>
	 * into a temporal path (under hadoop.tmp.dir) in the format of
	 * {@link SequenceFileOutputFormat}.
	 * <p>
	 * 
	 * Only the rows that meet the <code>criteria</code> will be 
	 * stored.  The <code>criteria</code> can only evaluate the 
	 * columns specified in the <code>projections</code>.
	 * 
	 * @param job 
	 * @param outputFormat
	 * @param criteria if specified (not null), only rows that satisfy the given <code>criteria</code>
	 * will be saved.  Note that, <code>criteria</code> is applied just before the persistant step, so
	 * it can only operate on the columns in the output schema of this job.
	 * @param projections the columns to be saved in the returned {@link Dataset}.
	 * @return a {@link Dataset} with the specified columns ()
	 * @throws IOException
	 */
	public Dataset build(MobiusJob job, Class<? extends FileOutputFormat> outputFormat, TupleCriterion criteria, Projectable... projections)
		throws IOException
	{
		return this.save(job, job.newTempPath(), outputFormat, criteria, projections);
	}
	
	
	
	/**
	 * Save the dataset and store the <code>projections</code>
	 * into a the specified <code>output</code> path in the 
	 * format of {@link TextOutputFormat}.
	 * <p>
	 * 
	 * <code>output</code> will be deleted before the job gets started.
	 */
	public Dataset save(MobiusJob job, Path output, Projectable... projections)
		throws IOException
	{
		return this.save(job, output, TextOutputFormat.class, null, projections);
	}
	
	
	/**
	 * Save the dataset and store the <code>projections</code>
	 * into a the specified <code>output</code> path in the 
	 * format of {@link TextOutputFormat}.
	 * <p>
	 * 
	 * Only the rows that meet the <code>criteria</code> will be 
	 * stored.  The <code>criteria</code> can only evaluate the 
	 * columns specified in the <code>projections</code>.
	 * <p>
	 * 
	 * <code>output</code> will be deleted before the job gets started.
	 */
	public Dataset save(MobiusJob job, Path output, TupleCriterion criteria, Projectable... projections)
		throws IOException
	{
		return this.save(job, output, TextOutputFormat.class, criteria, projections);
	}
	
	
	/**
	 * Save the dataset and store the <code>projections</code>
	 * into a the specified <code>output</code> path in the 
	 * format of the given <code>outputFormat</code>.
	 * <p>
	 * 
	 * <code>output</code> will be deleted before the job gets started.
	 */
	public Dataset save(MobiusJob job, Path output, Class<? extends FileOutputFormat> outputFormat, Projectable... projections)
		throws IOException
	{
		return this.save(job, output, outputFormat, null, projections);
	}
	
	
	/**
	 * Save the dataset and store the <code>projections</code>
	 * into a the specified <code>output</code> path in the 
	 * format of the given <code>outputFormat</code>.
	 * <p>
	 * 
	 * Only the rows that meet the <code>criteria</code> will be 
	 * stored.  The <code>criteria</code> can only evaluate the 
	 * columns specified in the <code>projections</code>.
	 * <p>
	 * 
	 * <code>output</code> will be deleted before the job gets started.
	 */
	public Dataset save(MobiusJob job, Path output, Class<? extends FileOutputFormat> outputFormat, TupleCriterion criteria, Projectable... projections)
		throws IOException
	{
		if( projections==null || projections.length==0 )
			throw new IllegalArgumentException("Please specify the output columns.");
		
		
		
		// - VALIDATION - make sure no ambiguous column names.
		//
		// make sure the projections don't have two or more different columns that
		// have the same name but in different dataset, as we are going the use 
		// the {@link Column#getOutputColumnName} as the output schema of the
		// returned dataset.
		Set<String> columnNames = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
		for( Projectable aColumn:projections )
		{			
			String[] outputSchema = aColumn.getOutputSchema();
			for(String anOutput:outputSchema)
			{
				if( !columnNames.contains(anOutput) )
				{
					columnNames.add(anOutput);
				}
				else
				{
					throw new IllegalArgumentException(columnNames+" from "+aColumn.toString()+" is ambiguous, it has the same name" +
							"as aother selected projected in different dataset, please use Column#setNewName(String) to" +
							"change it.");
				}
			}
		}
		
		// - VALIDATION - if <code>criteria</code> is not null, need to make
		// sure the columns used in the criteria are in the output columns.
		if ( criteria!=null )
		{
			TupleCriterion.validate(columnNames, criteria);
			this.jobConf.set(ConfigureConstants.PERSISTANT_CRITERIA, SerializableUtil.serializeToBase64(criteria));
		}
		
		
		// setup {@link Dataset} to {@link Column} mapping so we can setup projection columns
		// for each dataset, and also perform validation on making sure all the projection columns 
		// are from the selected <code>datasets</code> only,
		Map<Dataset, List<Column> > datasetToColumns = new HashMap<Dataset, List<Column>>();
		
		for( Projectable aFunc:projections )
		{
			Column[] requiredInputColumns = aFunc.getInputColumns();
			for( Column aColumn:requiredInputColumns )
			{
				Dataset aDataset = aColumn.getDataset();
				// make sure the <code>aDataset</code> within the participated datasets
				boolean withinSelectedDataset = false;
				for( Dataset aSelectedDataset:this.datasets )
				{
					if( aSelectedDataset.equals(aDataset) )
					{
						withinSelectedDataset = true;
						break;
					}
				}
				
				if( !withinSelectedDataset )
				{
					// user select a column from a dataset that doesn't
					// in the selected datasets in this join/group by job.
					throw new IllegalArgumentException(aColumn.toString()+" does not within the selected datasets " +
							"in this join/group task, please select columns only from the selected datasets.");
				}
				
				List<Column> projectablesInADataset = null;
				if ( (projectablesInADataset=datasetToColumns.get(aDataset))==null )
				{
					projectablesInADataset = new LinkedList<Column>();
					datasetToColumns.put(aDataset, projectablesInADataset);
				}
				
				if( !projectablesInADataset.contains(aColumn) )
					projectablesInADataset.add(aColumn);
			}
		}
		
		if( datasetToColumns.keySet().size()!=this.datasets.length )
		{
			throw new IllegalArgumentException("Please select at least one column from each dataset in the join/group-by job.");
		}
		
		// SETUP JOB
		if( this.userDefinedConf!=null )
		{
			this.jobConf = new JobConf(Util.merge(this.jobConf, this.userDefinedConf));
		}
		this.jobConf.setJarByClass(job.getClass());
		this.jobConf.setMapOutputKeyClass(DataJoinKey.class);
		this.jobConf.setMapOutputValueClass(DataJoinValue.class);
		this.jobConf.setPartitionerClass (DataJoinKeyPartitioner.class);
		this.jobConf.setOutputValueGroupingComparator (DataJoinKey.Comparator.class);
		this.jobConf.setOutputKeyComparatorClass (DataJoinKey.class);
		this.jobConf.setReducerClass(DefaultMobiusReducer.class);
		this.jobConf.set(ConfigureConstants.PROJECTION_COLUMNS, SerializableUtil.serializeToBase64(projections));
		
		
		
		JobSetup.setupOutputs(this.jobConf, output, outputFormat);
		
		// setup input paths, projection columns for each datasets.
		for( int jobSeq=0;jobSeq<this.datasets.length;jobSeq++)
		{
			Dataset aDataset = this.datasets[jobSeq];
			
			// setup input for each dataset
			JobSetup.setupInputs(jobConf, aDataset, jobSeq);
			
			// setup projection for each dataset
			JobSetup.setupProjections(jobConf, aDataset, jobSeq, datasetToColumns.get(aDataset).toArray(new Column[0]));
		}
		
		// setup all dataset IDs
		for( int i=0;i<this.datasets.length;i++)
		{
			String id = this.datasets[i].getDatasetID(i);
			if( !this.jobConf.get(ConfigureConstants.ALL_DATASET_IDS, "").isEmpty() )
			{
				this.jobConf.set(ConfigureConstants.ALL_DATASET_IDS, this.jobConf.get(ConfigureConstants.ALL_DATASET_IDS)+","+id);
			}
			else
			{
				this.jobConf.set(ConfigureConstants.ALL_DATASET_IDS, id);
			}
		}
		
		
		boolean isCombinable = true;
		for( Projectable aFunc:projections )
		{
			aFunc.setConf(jobConf);
			
			if( !aFunc.isCombinable() )
			{
				isCombinable = false;
			}
		}
		
		LOGGER.info("Using Combiner? "+isCombinable);
		if( isCombinable )
		{	
			jobConf.setCombinerClass(DefaultMobiusCombiner.class);
		}
		
		job.addToExecQueue(jobConf);
		
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
