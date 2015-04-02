package com.ebay.erl.mobius.core.datajoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;

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
 * @param <OK>
 * @param <OV>
 */
public class DataJoinOutputCollector<OK extends WritableComparable<?>, OV extends WritableComparable<?>> 
		implements OutputCollector<OK, OV>{

	private DataJoinMapper mapper;
	private OutputCollector<DataJoinKey, DataJoinValue> output;
	private Configuration conf;
	
	public DataJoinOutputCollector(DataJoinMapper mapper, OutputCollector<DataJoinKey, DataJoinValue> output, Configuration conf)
	{
		this.mapper = mapper;
		this.output = output;
		this.conf 	= conf;
	}
	
	@Override
	public void collect(OK key, OV value) throws IOException 
	{
		DataJoinKey nkey = new DataJoinKey(this.mapper.getDatasetID(), key, this.mapper.extractSortValueKeyword(value), this.mapper.getSortValueComparator());
		nkey.setConf(this.conf);
		
		DataJoinValue nvalue = new DataJoinValue(this.mapper.getDatasetID(), value);
		nvalue.setConf(this.conf);
		
		output.collect(nkey, nvalue);
	}

}
