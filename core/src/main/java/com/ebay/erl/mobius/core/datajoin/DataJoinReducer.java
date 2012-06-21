package com.ebay.erl.mobius.core.datajoin;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
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
 * @param <IK>
 * @param <IV>
 * @param <OK>
 * @param <OV>
 */
public abstract class DataJoinReducer <IK extends WritableComparable, 
									   IV extends WritableComparable, 
									   OK, OV> 
	extends MapReduceBase
	implements Reducer<DataJoinKey, DataJoinValue, OK, OV>{

	@Override
	public void reduce(DataJoinKey key, Iterator<DataJoinValue> values,
			OutputCollector<OK, OV> output, Reporter reporter) throws IOException {
		IK k = (IK)key.getKey();
		
		DataJoinValueGroup<IV> datajoinValues = new DataJoinValueGroup<IV>(values);
		
		joinreduce(k, datajoinValues, output, reporter);
	}
	
	
	public abstract void joinreduce(IK key, DataJoinValueGroup<IV> values, OutputCollector<OK, OV> output, Reporter reporter) throws IOException;
}
