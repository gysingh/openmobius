package com.ebay.erl.mobius.core.builder;

import java.io.IOException;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.ebay.erl.mobius.core.ConfigureConstants;
import com.ebay.erl.mobius.core.MobiusJob;
import com.ebay.erl.mobius.core.mapred.TSVMapper;

/**
 * Represents text-based and line-oriented files on HDFS.
 * <p>
 * 
 * Each line is delimited by the delimiter, ( the default
 * is tab), and the file is assigned the schema given in the 
 * constructor.
 * <p>
 * 
 * If the number of values in a line is less than the length 
 * of the schema, those columns are assigned a null value.
 * <p>
 * 
 * If the number of values in a line is greater than the length 
 * of the schema, those values are put into the tuple
 * with the name IDX_$i, where $i starts from the length of
 * the given schema.
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
 */
@SuppressWarnings({"deprecation", "unchecked"})
public class TSVDatasetBuilder extends AbstractDatasetBuilder<TSVDatasetBuilder>
{	
	protected TSVDatasetBuilder(MobiusJob job, String datasetName)
		throws IOException
	{
		super(job, datasetName);
		this.setDelimiter("\t");// set the default delimiter to tab,
	}
	
	
	/**
	 * Create a new instance of {@link TSVDatasetBuilder} to build 
	 * a text based dataset.
	 * <p>
	 * 
	 * By default, the underline text file will be read and delimited
	 * by tab in {@link TSVMapper}, then be converted into {@link Tuple}
	 * and set the given <code>schema</code>.  See {@link TSVMapper} for
	 * more detail.
	 * 
	 * @param job a Mobius job contains an analysis flow.
	 * @param name the name of the dataset to be built. 
	 * @param schema the schema of the underline dataset.
	 * @return
	 * @throws IOException
	 */
	public static TSVDatasetBuilder newInstance(MobiusJob job, String name, String[] schema)
		throws IOException
	{		
		TSVDatasetBuilder builder = new TSVDatasetBuilder(job, name);
		builder.setSchema(schema);
		return builder;
	}
	
	
	/**
	 * Specify the delimiter for the underline text file.
	 * <p>
	 * 
	 * The default delimiter is tab.
	 * 
	 * @param delimiter
	 * @return the {@linkplain TSVDatasetBuilder} itself.
	 * @throws IOException
	 */
	public TSVDatasetBuilder setDelimiter(String delimiter)
		throws IOException
	{
		((TSVDataset)this.getDataset()).setDelimiter(delimiter);
		return this;
	}

	@Override
	protected Dataset newDataset(String datasetName) 
	{	
		return new TSVDataset(this.mobiusJob, datasetName);
	}
	
	/**
	 * Change the default mapper implementation (default one is 
	 * {@link TSVMapper}), user should call this mapper when
	 * the parsing logic in {@link TSVMapper} doesn't meet
	 * the requirement. 
	 * 
	 * @param mapper
	 * @return
	 */
	public TSVDatasetBuilder setMapper(Class<? extends TSVMapper> mapper)
	{
		((TSVDataset)this.getDataset()).setMapper(mapper);
		return this;
	}



	/**
	 * {@inheritDoc}
	 */
	@Override
	public Dataset buildFromPreviousJob(JobConf prevJob,
			Class<? extends FileOutputFormat> prevJobOutputFormat,
			String[] schema) 
		throws IOException
	{
		if ( prevJobOutputFormat.equals(TextOutputFormat.class) )
		{
			// no need to validate the input path as it's coming from 
			// previous dataset
			this.addInputPath(false, FileOutputFormat.getOutputPath(prevJob));
			this.setSchema(schema);
			this.setDelimiter(prevJob.get(ConfigureConstants.TUPLE_TO_STRING_DELIMITER, "\t"));
			return this.build();
		}
		else
		{
			throw new IllegalArgumentException(this.getClass().getCanonicalName()+" cannot build dataset from "+prevJobOutputFormat.getCanonicalName()+
					", only "+TextOutputFormat.class.getCanonicalName()+" is supported.");
		}
	}
}
