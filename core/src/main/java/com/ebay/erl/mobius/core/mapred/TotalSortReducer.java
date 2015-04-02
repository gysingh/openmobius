package com.ebay.erl.mobius.core.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.ebay.erl.mobius.core.datajoin.DataJoinValueGroup;
import com.ebay.erl.mobius.core.function.base.Projectable;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;

/**
 * Reducer for mobius total sort job.
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
@SuppressWarnings("deprecation")
public class TotalSortReducer extends DefaultMobiusReducer
{	
	@Override
	public void configure(JobConf conf)
	{
		super.configure(conf);
	}
	
	/**
	 * reduce()
	 * <p>
	 * 
	 * Output key is {@link org.apache.hadoop.io.NullWritable} and output 
	 * value is {@link com.ebay.erl.mobius.core.model.Tuple}
	 */
	@Override
	public void joinreduce(Tuple key, DataJoinValueGroup<Tuple> values, OutputCollector<NullWritable, WritableComparable<?>> output, Reporter reporter)
		throws IOException
	{	
		if( values.hasNext() )
		{
			Byte datasetID = values.nextDatasetID ();
			Iterator<Tuple> valuesToBeOutput = values.next();
			while( valuesToBeOutput.hasNext() )
			{
				Tuple outTuple = new Tuple();
				
				Tuple aTuple = valuesToBeOutput.next();
				aTuple.setSchema(this.getSchemaByDatasetID(datasetID));
				
				// make the column output ordering the same as
				// the <code>_projections</code> ordering.
				for( Projectable aFunc:this._projections)
				{
					String name = ((Column)aFunc).getInputColumnName();					
					outTuple.insert(name, aTuple.get(name));
				}				
				output.collect(NullWritable.get(), outTuple);
			}
		}
	}
}
