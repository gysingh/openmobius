package com.ebay.erl.mobius.core.builder;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import com.ebay.erl.mobius.core.MobiusJob;
import com.ebay.erl.mobius.core.mapred.DefaultSeqFileMapper;
import com.ebay.erl.mobius.core.mapred.SequenceFileMapper;
import com.ebay.erl.mobius.core.model.Tuple;


/**
 * Reads a {@link SequenceFile} with {@linkplain NullWritable}
 * as its key and {@linkplain Tuple} as its value.
 * <p>
 * 
 * The default <code>Mapper</code> is {@link DefaultSeqFileMapper} which only
 * accepts {@linkplain NullWritable} as the key type and {@linkplain Tuple} as
 * the value type from the underline sequence file.
 * <p>
 * 
 * If the sequence file includes different key and value types, specify
 * a different implementation of {@link SequenceFileMapper} using 
 * {@link #setMapper(Class)}.
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
public class SeqFileDatasetBuilder extends AbstractDatasetBuilder<SeqFileDatasetBuilder>
{
	/**
	 * Mapper class of this builder,  by default, it's {@link DefaultSeqFileMapper}
	 */
	protected Class<? extends SequenceFileMapper> mapperClass = DefaultSeqFileMapper.class;
	
	protected SeqFileDatasetBuilder(MobiusJob aJob, String datasetName) 
	{
		super(aJob, datasetName);
	}
	
	/**
	 * Get an new instance of {@link SeqFileDatasetBuilder} to build a dataset
	 * which is stored as Hadoop sequence file.
	 * <p>
	 * 
	 * By default, a {@linkplain SeqFileDatasetBuilder} use {@link DefaultSeqFileMapper} 
	 * to parse the underline sequence file records into {@linkplain Tuple}s, and
	 * the <code>schema</code> is set to every {@link Tuple}.
	 * <p>
	 * 
	 * Please note that, the <code>schema</code> is not the names given to the key and 
	 * value in the sequence file, but the names to the parsed results ({@linkplain Tuple}s).
	 * 
	 * @param job a Mobius job contains the analysis flow.
	 * @param name the name of the dataset to be build.
	 * @param schema the schema of this dataset.
	 */
	public static SeqFileDatasetBuilder newInstance(MobiusJob job, String name, String[] schema)
		throws IOException
	{		
		SeqFileDatasetBuilder builder = new SeqFileDatasetBuilder(job, name);
		builder.getDataset().setSchema(schema);
		return builder;
	}
	
	
	/**
	 * Set a new implementation of {@link SequenceFileMapper} to parse the underline
	 * sequence file records into tuples.
	 */
	public SeqFileDatasetBuilder setMapper(Class<? extends SequenceFileMapper> mapperClass)
	{
		this.mapperClass = mapperClass;
		this.getDataset().setMapper(mapperClass);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected Dataset newDataset(String datasetName)
	{
		return new SeqFileDataset(this.mobiusJob, datasetName, this.mapperClass);
	}
	
	
	/**
	 * {@inheritDoc}
	 * 
	 * If <code>prevJobOutputFormat</code> is {@link SequenceFileOutputFormat},
	 * Mobius will use this class to build a dataset from the <code>prevJob</code>,
	 * which is an intermediate results in a Mobius job.
	 */
	@Override
	public Dataset buildFromPreviousJob(JobConf prevJob,
			Class<? extends FileOutputFormat> prevJobOutputFormat,
			String[] schema) 
		throws IOException 
	{
		if( prevJobOutputFormat.equals(SequenceFileOutputFormat.class))
		{
			this.addInputPath(false, FileOutputFormat.getOutputPath(prevJob));// no need validation
			this.setSchema(schema);
			return this.build();
		}
		else
		{
			throw new UnsupportedOperationException(this.getClass().getCanonicalName()+ " only support SequenceFileOutputFormat.");
		}
	}	
}
