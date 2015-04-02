package com.ebay.erl.mobius.core.datajoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * <p>
 * This product is licensed under the Apache License,  Version 2.0, 
 * available at http://www.apache.org/licenses/LICENSE-2.0.
 * 
 * This product contains portions derived from Apache hadoop which is 
 * licensed under the Apache License, Version 2.0, available at 
 * http://hadoop.apache.org.
 * 
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Jack Shen, Gyanit Singh, Neel Sundaresan
 *
 * @param <K>
 * @param <V>
 */
@SuppressWarnings("deprecation")
public class DataJoinKeyPartitioner<K extends WritableComparable<K>,V extends WritableComparable<V>> 
		implements Partitioner<DataJoinKey, DataJoinValue>{

	@SuppressWarnings("unchecked")
	@Override
	public int getPartition(DataJoinKey key, DataJoinValue value, int numPartitions) 
	{
		WritableComparable wKey	= key.getKey();
		int hashCode			= wKey.hashCode();
		int partition			= (hashCode & Integer.MAX_VALUE) % numPartitions;
		return partition;
	}

	@Override
	public void configure(JobConf job) {
		
	}

}
