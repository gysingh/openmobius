package com.ebay.erl.mobius.core.mapred;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MobiusDelegatingInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import com.ebay.erl.mobius.core.ConfigureConstants;

/**
 * {@link MultipleInputs} uses the input path to find the corresponding 
 * Map class.  Mobius need to find the {@link com.ebay.erl.mobius.core.criterion.TupleCriterion}, 
 * schema, or projection column for a given {@link com.ebay.erl.mobius.core.builder.Dataset}, 
 * and two different {@link com.ebay.erl.mobius.core.builder.Dataset} elements in a Mobius job 
 * might be processed by same type of {@link AbstractMobiusMapper}. For example, joining two 
 * different CSV dataset, both {@link com.ebay.erl.mobius.core.builder.Dataset} are processed by
 * {@link com.ebay.erl.mobius.core.mapred.TSVMapper}.
 * <p>
 * 
 * This class is used to build the path to dataset mapping, or vice versa, so
 * the mapper can use {@link com.ebay.erl.mobius.core.ConfigureConstants#CURRENT_DATASET_ID} to 
 * determine the projection columns, filters, and so on.
 * <p>
 * 
 * This class is used along with {@link MobiusDelegatingInputFormat}
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
 * 
 * @see MobiusDelegatingInputFormat
 */
@SuppressWarnings({ "unchecked", "deprecation" })
public class MobiusMultiInputs 
{	
	public static void addInputPath(JobConf conf, Path anInput, Class<? extends InputFormat> inputFormatClass,
			Class<? extends AbstractMobiusMapper> mapperClass, byte datasetID, FileSystem fs)
		throws IOException
	{
		MultipleInputs.addInputPath (conf, anInput, inputFormatClass, mapperClass);
			
		// override the {@link InputFormat} class set by the {@link MultipleInputs}
		// as Mobius need to set the set the current dataset id per input split.
		conf.setInputFormat (MobiusDelegatingInputFormat.class);
			
		// MobiusDelegatingInputFormat extends DelegatingInputFormat, which always
		// call the FileInpupt#setInputs within DelegatingInputFormat#getInputs
		// regardless of the actual type of <code>inputFormatClass</code>.
		
			
			
		/////////////////////////////////////////////////////
		// start to build the path to dataset ID mapping
		/////////////////////////////////////////////////////
		MultiInputsHelper helper 	= MultiInputsHelpersRepository.getInstance(conf).getHelper(inputFormatClass);
		URI uri						= helper.getUniquePathByInputFormat(conf, anInput);
		String aPath 				= uri.toString();
		
		if( aPath.indexOf(";")>=0 )
			throw new IllegalArgumentException(aPath+" cannot contains semicolon");
			
		// set the input path to datasetID mapping in the Hadoop configuration.
		if( conf.get(ConfigureConstants.INPUT_TO_DATASET_MAPPING, "").isEmpty() )
		{
			conf.set(ConfigureConstants.INPUT_TO_DATASET_MAPPING, datasetID+";"+aPath);
		}
		else
		{
			String previous = conf.get(ConfigureConstants.INPUT_TO_DATASET_MAPPING);
			conf.set(ConfigureConstants.INPUT_TO_DATASET_MAPPING, datasetID+";"+aPath+","+previous);
		}
		
		//LOGGER.debug(conf.get(ConfigureConstants.INPUT_TO_DATASET_MAPPING, ""));
	}
}
