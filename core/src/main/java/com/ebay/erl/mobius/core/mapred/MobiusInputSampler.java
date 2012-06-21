package com.ebay.erl.mobius.core.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MobiusDelegatingInputFormat;
import org.apache.hadoop.mapred.lib.InputSampler.Sampler;
import org.apache.hadoop.util.ReflectionUtils;

import com.ebay.erl.mobius.core.ConfigureConstants;
import com.ebay.erl.mobius.core.datajoin.DataJoinKey;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.core.sort.Sorter;
import com.ebay.erl.mobius.core.sort.Sorter.Ordering;
import com.ebay.erl.mobius.util.SerializableUtil;
import com.ebay.erl.mobius.util.Util;

/**
 * Performing sampling for total sort job.
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
 */
@SuppressWarnings({ "deprecation", "unchecked" })
public class MobiusInputSampler implements Sampler {
	private double freq;
	private final int numSamples;
	private final int maxSplitsSampled;
	
	private static final Log LOGGER = LogFactory.getLog(MobiusInputSampler.class);

	public MobiusInputSampler(double freq, int numSamples, int maxSplitsSampled) 
	{		
		this.freq = freq;
		this.numSamples = numSamples;
		this.maxSplitsSampled = maxSplitsSampled;
	}
	
	private AbstractMobiusMapper getMapper(InputFormat inf, InputSplit split, JobConf conf)
		throws IOException
	{
		AbstractMobiusMapper mapper = null;
		if( inf instanceof MobiusDelegatingInputFormat)
		{
			Class<AbstractMobiusMapper> mapperClass = ((MobiusDelegatingInputFormat)inf).getMapper(split, conf);
			mapper = ReflectionUtils.newInstance(mapperClass, conf);
		}
		else
		{
			Class<? extends AbstractMobiusMapper> mapperClass = (Class<? extends AbstractMobiusMapper>) Util.getClass(conf.get(ConfigureConstants.MAPPER_CLASS));
			mapper = ReflectionUtils.newInstance(mapperClass, conf);
		}
		return mapper;
	}

	@Override
	public Object[] getSample(InputFormat inf, JobConf job)
		throws IOException 
	{
		// the following codes are copied from {@link InputSampler#RandomSampler},
		// but require some modifications.
		
		InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());
		ArrayList<DataJoinKey> samples = new ArrayList<DataJoinKey>(this.numSamples);
		int splitsToSample = Math.min(this.maxSplitsSampled, splits.length);

		Random r = new Random();
		long seed = r.nextLong();
		r.setSeed(seed);
		
		
		
		
		// get Sorters
		Sorter[] sorters = null;
		if( job.get(ConfigureConstants.SORTERS, null)!=null )
		{
			// total sort job
			sorters = (Sorter[])SerializableUtil.deserializeFromBase64(job.get(ConfigureConstants.SORTERS), job);
		}
		else
		{
			// there is no sorter, should be reducer/join job
			Column[] keys = (Column[])SerializableUtil.deserializeFromBase64(job.get(ConfigureConstants.ALL_GROUP_KEY_COLUMNS), job);
			sorters = new Sorter[keys.length];
			for( int i=0;i<keys.length;i++ )
			{
				sorters[i] = new Sorter(keys[i].getInputColumnName(), Ordering.ASC);
			}
		}
		
		long proportion = 10L;
		while( (int)(this.freq*proportion)==0 ){
			proportion = proportion*10;
		}
		proportion = 5L*proportion;

		// shuffle splits
		for (int i = 0; i < splits.length; ++i) 
		{
			InputSplit tmp = splits[i];
			int j = r.nextInt(splits.length);
			splits[i] = splits[j];
			splits[j] = tmp;
		}

		SamplingOutputCollector collector = new SamplingOutputCollector();
		for (int i = 0; i < splitsToSample
				|| (i < splits.length && samples.size() < numSamples); i++) 
		{
			LOGGER.info("Sampling from split #"+(i+1)+", collected samples:"+samples.size());
			
			
			RecordReader<WritableComparable, WritableComparable> reader = inf.getRecordReader(splits[i], job, Reporter.NULL);
			WritableComparable key			= reader.createKey();
			WritableComparable value 		= reader.createValue();
			
			if( !(inf instanceof MobiusDelegatingInputFormat) )
			{
				// not mobius delegating input format, so the CURRENT_DATASET_ID
				// will not be set by inf#getRecordReader, we set them here.
				//
				// set the current dataset id, as the AbstractMobiusMapper#configure
				// method needs this property.
				job.set (ConfigureConstants.CURRENT_DATASET_ID, job.get(ConfigureConstants.ALL_DATASET_IDS));
			}
			
			Text datasetID = new Text(job.get(ConfigureConstants.CURRENT_DATASET_ID));	
			LOGGER.info("Samples coming from dataset: "+datasetID.toString());
			AbstractMobiusMapper mapper = this.getMapper(inf, splits[i], job);
			mapper.configure(job);
			
			// reading elements from one split
			long readElement = 0;
			while (reader.next(key, value))
			{
				collector.clear();
				Tuple tuple = mapper.parse(key, value);
				
				readElement++;
				if (readElement> (((long)numSamples)*((long)proportion)) )
				{
					// a split might be very big (ex: a large gz file),
					// so we just need to read the 
					break;
				}
				
				if (r.nextDouble() <= freq) 
				{
					if (samples.size() < numSamples) 
					{
						mapper.joinmap(key, value, collector, Reporter.NULL);
						// joinmap function might generate more than one output key
						// per <code>key</code> input. 
						for( Tuple t:collector.getOutKey() )
						{
							Tuple mt = Tuple.merge(tuple, t);
							DataJoinKey nkey = this.getKey(mt, sorters, datasetID, mapper, job);
							samples.add(nkey);
						}
					} 
					else 
					{
						// When exceeding the maximum number of samples, replace
						// a random element with this one, then adjust the
						// frequency to reflect the possibility of existing 
						// elements being pushed out
						
						mapper.joinmap(key, value, collector, Reporter.NULL);
						for( Tuple t:collector.getOutKey() )
						{
							int ind = r.nextInt(numSamples);
							if (ind != numSamples) 
							{
								Tuple mt = Tuple.merge(tuple, t);
								DataJoinKey nkey = this.getKey(mt, sorters, datasetID, mapper, job);
								samples.set(ind, nkey);
							}
						}
						
						freq *= (numSamples - collector.getOutKey().size()) / (double) numSamples;
					}
					key		= reader.createKey();
					value	= reader.createValue();
				}
			}
			reader.close();
		}
		LOGGER.info("Samples have been collected, return.");
		return samples.toArray();
	}
	
	
	private DataJoinKey getKey(Tuple tuple, Sorter[] sorter, Text datasetID, AbstractMobiusMapper mapper, Configuration conf)
	{
		Tuple columnsUsedToSort = new Tuple();
		for(Sorter aSorter:sorter )
		{
			String name		= aSorter.getColumn();
			Object value	= tuple.get(name);
			columnsUsedToSort.insert(name, value);
		}
		
		DataJoinKey nkey = new DataJoinKey(datasetID, columnsUsedToSort, mapper.extractSortValueKeyword(tuple), mapper.getSortValueComparator());
		nkey.setConf(conf);
		return nkey;
	}
	
	
	private static class SamplingOutputCollector implements OutputCollector<Tuple, Tuple>
	{	
		private List<Tuple> keys = new ArrayList<Tuple>();
		
		@Override
		public void collect(Tuple key, Tuple value) throws IOException 
		{
			this.keys.add(key);
		}
		
		// to be called for every new key
		public void clear(){
			this.keys.clear();
		}
		
		public List<Tuple> getOutKey(){
			return this.keys;
		}		
	}
}
