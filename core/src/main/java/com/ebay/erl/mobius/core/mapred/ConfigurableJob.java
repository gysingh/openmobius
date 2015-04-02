package com.ebay.erl.mobius.core.mapred;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapred.lib.InputSampler.Sampler;

import com.ebay.erl.mobius.core.datajoin.DataJoinKey;
import com.ebay.erl.mobius.core.datajoin.EvenlyPartitioner;
import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.util.Util;

/**
 * Catching the total sort job submission, performing
 * sampling before submit the total sort job.
 * <p>
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
@SuppressWarnings({"deprecation","unchecked"})
public class ConfigurableJob extends Job
{

	private static final Log LOGGER = LogFactory.getLog (ConfigurableJob.class);
	
	private static final DecimalFormat format = new DecimalFormat("#.###");
	
	public ConfigurableJob(JobConf jobConf)
		throws IOException 
	{
		super(jobConf);	
	}	
	
	
	@Override
	protected synchronized void submit()
	{	
		JobConf jobConf = this.getJobConf();
		boolean isLocalHadoop = jobConf.get("mapred.job.tracker", "local").equals("local");
		
		// the default partitioner is {@link com.ebay.erl.mobius.core.datajoin.DataJoinKeyPartitioner}
		// which is hash based.
		//
		// If user choose to use even partitioner, Mobius will use
		// {@link com.ebay.erl.mobius.core.datajoin.EvenlyPartitioner} which
		// is sampling based partitioner of attempting to balance the load
		// for each reducer.
		String partitioner = jobConf.get("mobius.partitioner", "default");
		
		if(	!isLocalHadoop && jobConf.getNumReduceTasks()!=0 && partitioner.equals("even")  )
		{	
			// this job needs reducer, perform sampling on the keys to 
			// make load on reducers are almost evenly distributed.
			
			double freq		= jobConf.getFloat ("mobius.sampler.freq", 0.1F);
			int numSamples	= jobConf.getInt ("mobius.sampler.num.samples", 50000);
			int maxSplits	= jobConf.getInt ("mobius.sampler.max.slipts.sampled", 5);
			
			// log sampling parameters so that user knows.
			LOGGER.info("Sampling parameters { " +
					"mobius.sampler.freq:"+format.format(freq)+", " +
							"mobius.sampler.num.samples:"+numSamples+", " +
									"mobius.sampler.max.slipts.sampled:"+maxSplits+"}");
			
			InputSampler.Sampler<?, ?> sampler = new MobiusInputSampler(freq, numSamples, maxSplits);
		
			writePartitionFile(jobConf, sampler);
			
			// add to distributed cache
			try
			{	
				URI partitionUri = new URI (TotalOrderPartitioner.getPartitionFile (jobConf) + "#_partitions");
				LOGGER.info("Adding partition uri to distributed cache:"+partitionUri.toString());	
									
				DistributedCache.addCacheFile (partitionUri, jobConf);
				DistributedCache.createSymlink (jobConf);
				jobConf.setPartitionerClass (EvenlyPartitioner.class);
					
				LOGGER.info("Using "+EvenlyPartitioner.class.getCanonicalName()+" to partiton the keys evenly among reducers.");
			}
			catch ( URISyntaxException e )
			{
				LOGGER.error(e.getMessage(), e);
				throw new RuntimeException(e);
			}
			
			// adding -XX:-UseParallelOldGC, this will automatically set -XX:-UseParallelGC
			// according to Oracle's specification
			String jvmOpts = jobConf.get("mapred.child.java.opts", "");
			if( jvmOpts.isEmpty() ){
				jvmOpts = "-XX:-UseParallelOldGC";
			}
			else{
				if ( jvmOpts.indexOf("-XX:-UseParallelOldGC")<0 )
				{
					// remove "
					jvmOpts = jvmOpts.replaceAll("\"", "");
					jvmOpts = jvmOpts.concat(" -XX:-UseParallelOldGC");
				}
			}
			jobConf.set("mapred.child.java.opts", jvmOpts);
						
			this.setJobConf(jobConf);
		}
		LOGGER.info("Submiting job:"+jobConf.getJobName());
		super.submit();
	}
	
	
	
	
	
	private static void writePartitionFile(JobConf job, Sampler sampler)
	{
		try
		{
			////////////////////////////////////////////////
			// first, getting samples from the data sources
			////////////////////////////////////////////////
			LOGGER.info("Running local sampling for job ["+job.getJobName()+"]");			
			InputFormat inf		= job.getInputFormat ();
			Object[] samples	= sampler.getSample (inf, job);
			LOGGER.info("Samples retrieved, sorting...");
	
			////////////////////////////////////////////////
			// sort the samples
			////////////////////////////////////////////////
			RawComparator comparator = job.getOutputKeyComparator ();	
			Arrays.sort (samples, comparator);
			
			if( job.getBoolean("mobius.print.sample", false) )
			{
				PrintWriter pw = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(new File(job.get("mobius.sample.file", "./samples.txt.gz")))))));
				for( Object obj:samples )
				{
					pw.println(obj);
				}
				pw.flush();
				pw.close();
			}
			
			////////////////////////////////////////////////
			// start to write partition files
			////////////////////////////////////////////////
						
			FileSystem fs 		= FileSystem.get (job);
			Path partitionFile	= fs.makeQualified(new Path (TotalOrderPartitioner.getPartitionFile (job)));
			while ( fs.exists(partitionFile) ) 
			{
				partitionFile = new Path(partitionFile.toString()+"."+System.currentTimeMillis());
			}
			fs.deleteOnExit(partitionFile);
			TotalOrderPartitioner.setPartitionFile(job, partitionFile);
			LOGGER.info("write partition file to:" + partitionFile.toString());
			
			int reducersNbr = job.getNumReduceTasks ();
			Set<Object> wroteSamples = new HashSet<Object>();

			SequenceFile.Writer writer = SequenceFile.createWriter(fs, job, partitionFile, Tuple.class, NullWritable.class);

			
			float avgReduceSize = samples.length / reducersNbr;
			
			

			int lastBegin = 0;
			for( int i=0;i<samples.length;)
			{
				// trying to distribute the load for every reducer evenly,
				// dividing the <code>samples</code> into a set of blocks
				// separated by boundaries, objects that selected from the
				// <code>samples</code> array, and each blocks should have
				// about the same size.
				
				// find the last index of element that equals to samples[i], as
				// such element might appear multiple times in the samples.
				int upperBound		= Util.findUpperBound(samples, samples[i], comparator);
				
				int lowerBound		= i;//Util.findLowerBound(samples, samples[i], comparator);
				
				// the repeat time of samples[i], if the key itself is too big
				// select it as boundary
				int currentElemSize = upperBound-lowerBound + 1;
				
				
				if( currentElemSize>avgReduceSize*2) // greater than two times of average reducer size
				{
					// the current element is too big, greater than
					// two times of the <code>avgReduceSize</code>, 
					// put itself as boundary
					writer.append( ((DataJoinKey)samples[i]).getKey(), NullWritable.get());
					wroteSamples.add(((DataJoinKey)samples[i]).getKey());					
					//pw.println(samples[i]);
					
					// immediate put the next element to the boundary,
					// the next element starts at <code> upperBound+1
					// </code>, to prevent the current one consume even 
					// more.
					if( upperBound+1<samples.length )
					{
						writer.append( ((DataJoinKey)samples[upperBound+1]).getKey(), NullWritable.get());
						wroteSamples.add( ((DataJoinKey)samples[upperBound+1]).getKey() );					
						//pw.println(samples[upperBound+1]);
						
						// move on to the next element of <code>samples[upperBound+1]/code>
						lastBegin = Util.findUpperBound(samples, samples[upperBound+1], comparator)+1;
						i = lastBegin;
					}
					else
					{
						break;
					}
				}
				else
				{
					// current element is small enough to be consider
					// with previous group
					int size = upperBound - lastBegin;
					if( size>avgReduceSize )
					{
						// by including the current elements, we have
						// found a block that's big enough, select it
						// as boundary
						writer.append( ((DataJoinKey)samples[i]).getKey(), NullWritable.get());
						wroteSamples.add( ((DataJoinKey)samples[i]).getKey() );					
						//pw.println(samples[i]);
						
						i = upperBound+1;
						lastBegin = i;
					}
					else
					{
						i = upperBound+1;
					}
				}
			}
			
			writer.close();
			

			// if the number of wrote samples doesn't equals to number of
			// reducer minus one, then it means the key spaces is too small
			// hence TotalOrderPartitioner won't work, it works only if 
			// the partition boundaries are distinct.
			//
			// we need to change the number of reducers
			if (wroteSamples.size()+1 != reducersNbr) 
			{
				LOGGER.info("Write complete, but key space is too small, sample size="
								+ wroteSamples.size()
								+ ", reducer size:"
								+ (reducersNbr));
				LOGGER.info("Set the reducer size to:" + (wroteSamples.size() + 1));

				// add 1 because the wrote samples define boundary, ex, if
				// the sample size is two with two element [300, 1000], then 
				// there should be 3 reducers, one for handling i<300, one 
				// for n300<=i<1000, and another one for 1000<=i
				job.setNumReduceTasks((wroteSamples.size() + 1));
			}
			
			samples = null;
		}
		catch(IOException e)
		{
			LOGGER.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}
}
