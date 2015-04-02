package com.ebay.erl.mobius.core.mapred;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.ebay.erl.mobius.core.builder.Dataset;

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
 */
@SuppressWarnings( { "deprecation" })
public abstract class MultiInputsHelper
{
	public MultiInputsHelper(){}
	
	/**
	 * Get a unique {@link URI} so that Mobius can use the URI to locate the
	 * corresponding {@link Dataset}.
	 * 
	 * @param conf
	 * @param anInput
	 * @param mapperClass
	 * @return
	 */
	public abstract URI getUniquePathByInputFormat(JobConf conf, Path anInput)
		throws IOException;
	
	/**
	 * Get the URI to for Mobius to identify the current dataset ID.  Return null
	 * if the <code>split</code> cannot be handled by this helper. 
	 * 
	 * @param split
	 * @param conf
	 * @return
	 */
	public abstract URI getPathBySplit(InputSplit split, JobConf conf)
		throws IOException;
}
