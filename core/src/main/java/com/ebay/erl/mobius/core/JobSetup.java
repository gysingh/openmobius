package com.ebay.erl.mobius.core;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.mapred.MobiusMultiInputs;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;

/**
 * A utility class for Mobius engine
 * to setup parameters to construct
 * a Mobius job.
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
class JobSetup 
{
	private static final Log LOGGER = LogFactory.getLog(JobSetup.class);
	
	
	/**
	 * specify the columns that a mapper needs to emit.
	 */
	public static void setupProjections(JobConf job, Dataset dataset, int jobSeqNbr, Column... projections)
	{	
		StringBuffer sortedColumns = new StringBuffer();
		
		// dedupe the projection input column name and then sort it.
		
		Set<String> uniqueColumnNames = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
		
		for(Column aProjection:projections)
		{
			uniqueColumnNames.add(aProjection.getInputColumnName());
		}
		
		Iterator<String> it = uniqueColumnNames.iterator();
		while( it.hasNext() )
		{
			sortedColumns.append(it.next());
			if( it.hasNext() )
				sortedColumns.append(",");
		}
		job.set(dataset.getDatasetID(jobSeqNbr)+".value.columns", sortedColumns.toString());
		
		// for Mapper only task
		StringBuffer originalOrder = new StringBuffer();
		for( int i=0;i<projections.length;i++ )
		{
			originalOrder.append(projections[i].getInputColumnName());
			if( i<projections.length-1 )
				originalOrder.append(",");
		}
		job.set(dataset.getDatasetID(jobSeqNbr)+".columns.in.original.order", originalOrder.toString());
	}
	
	
	
	/**
	 * validate if the given </code>columns</code> are all within the dataset,
	 * this is for preventing user to select a column C from anther dataset D,
	 * but D diesn't participate in a Mobius task, such as join, list or group_by.
	 * 
	 */
	public static void validateColumns(Dataset dataset, Column...columns)
	{
		for( Column aColumn:columns )
		{
			if( !dataset.equals(aColumn.getDataset()) )
			{
				throw new IllegalArgumentException("Column["+aColumn.getInputColumnName()+"] belongs to "+aColumn.getDataset()+", " +
						"please select the column from "+dataset.toString());
			}
		}
	}
	
	
	
	/**
	 * Setup the input path(s) for the given <code>job</code>.
	 * <p>
	 * 
	 * The input path(s) is retrieved from <code>aDataset</code>,
	 * and the <code>jobSequenceNbr</code> is for indicating
	 * the job sequence.
	 */
	public static void setupInputs(JobConf job, Dataset aDataset, int jobSequenceNbr)
		throws IOException
	{
		for ( Path anInput:aDataset.getInputs() )
		{
			MobiusMultiInputs.addInputPath (
					job, 
					anInput, 
					aDataset.getInputFormat (), 
					aDataset.getMapper (), 
					aDataset.getDatasetID (jobSequenceNbr), 
					FileSystem.get (job)
			);
		}
	}
	
	
	
	/**
	 * Setup the output path of the given <code>job</code> to
	 * <code>outputFolder</code> with the given 
	 * <code>outputFormat</code>.
	 */
	public static void setupOutputs(JobConf job, Path outputFolder, Class<? extends FileOutputFormat> outputFormat)
		throws IOException
	{
		FileOutputFormat.setOutputPath(job, outputFolder);
		job.setClass("mapred.output.format.class", outputFormat, FileOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Tuple.class);
		
		ensureOutputDelete(outputFolder, job);		
	}
	
	private static void ensureOutputDelete(Path outputFolder, Configuration conf)
		throws IOException
	{
		FileSystem fs = FileSystem.get(conf);
		outputFolder = fs.makeQualified(outputFolder);
		if( fs.exists(outputFolder) )
		{
			LOGGER.info("Deleting "+outputFolder.toString());
			fs.delete(outputFolder, true);
		}
	}
}
