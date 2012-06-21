package com.ebay.erl.mobius.core.mapred;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.core.builder.SeqFileDatasetBuilder;


/**
 * A default implementation of SequenceFileMapper supporting sequence 
 * file with {@linkplain NullWritable} as its key type and 
 * {@linkplain Tuple} as its value type.  Any other type throws
 * IllegalArgumentException in the {@linkplain #parse(Writable, Writable)}
 * method.
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
public class DefaultSeqFileMapper extends SequenceFileMapper<NullWritable, Tuple>
{

	/**
	 * read the <code>invalue</code> and set the <code>schema</code>
	 * to the returned {@link Tuple}.  The <code>schema</code> is 
	 * specified in {@link SeqFileDatasetBuilder#newInstance(com.ebay.erl.mobius.core.MobiusJob, String, String[])}
	 */
	@Override
	public Tuple parse(NullWritable inkey, Tuple invalue)
			throws IllegalArgumentException, IOException 
	{
		Tuple result = (Tuple)invalue;
		result.setSchema(this.getSchema());
		return result;
	}
}
