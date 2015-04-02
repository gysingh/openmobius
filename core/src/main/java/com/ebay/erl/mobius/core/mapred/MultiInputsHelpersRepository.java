package com.ebay.erl.mobius.core.mapred;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import com.ebay.erl.mobius.util.ClassComparator;
import com.ebay.erl.mobius.util.Util;


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
@SuppressWarnings({"deprecation", "unchecked"})
public class MultiInputsHelpersRepository 
{
	private Map<Class<? extends InputFormat>, MultiInputsHelper> mapping;
	
	private static MultiInputsHelpersRepository _INSTANCE = null;
	
	private static Log LOGGER = LogFactory.getLog(MultiInputsHelpersRepository.class);
	
	/**
	 * constructor
	 */
	private MultiInputsHelpersRepository(JobConf conf)
	{
		this.mapping = new TreeMap<Class<? extends InputFormat>, MultiInputsHelper>(new ClassComparator());
		
		this.register(FileInputFormat.class, FileInputFormatHelper.class);
		
		if( !conf.get("mobius.multi.inputs.helpers", "").isEmpty() )
		{
			// mobius.multi.inputs.helpers in the format of> InputFormatClassName:HelperClassName(,InputFormatClassName:HelperClassName)?
			String[] helpers = conf.getStrings("mobius.multi.inputs.helpers");
			for( String aHeler:helpers )
			{
				String[] data				= aHeler.split(":");
				String inputFormatClassName = data[0];
				String helperClassName		= data[1];
					
				Class<? extends InputFormat> inputFormat		= (Class<? extends InputFormat>)Util.getClass(inputFormatClassName);
				Class<? extends MultiInputsHelper> helperClass	= (Class<? extends MultiInputsHelper>)Util.getClass(helperClassName);
					
				this.register(inputFormat, helperClass);
			}
		} 
	}
	
	/**
	 * 
	 */
	public static MultiInputsHelpersRepository getInstance(JobConf conf)
	{
		if( MultiInputsHelpersRepository._INSTANCE==null )
		{
			MultiInputsHelpersRepository._INSTANCE = new MultiInputsHelpersRepository(conf);
		}
		
		return MultiInputsHelpersRepository._INSTANCE;
	}
	
	/**
	 * get the URI from an input split to locate the current dataset ID 
	 */
	public URI getURIBySplit(InputSplit split, JobConf conf)
		throws IOException
	{
		for( MultiInputsHelper aHelper:this.mapping.values() )
		{
			URI result = aHelper.getPathBySplit(split, conf);
			if( result!=null )
				return result;
		}
		
		throw new IllegalArgumentException("Cannod find "+MultiInputsHelper.class.getSimpleName()+" using "+split.getClass().getCanonicalName());
	}
	
	public MultiInputsHelper getHelper(Class<? extends InputFormat> inputFormat)
	{
		// get the closest helper
		Iterator<Class<? extends InputFormat>> it = this.mapping.keySet().iterator();
		
		while( it.hasNext() )
		{
			Class<? extends InputFormat> anRegisteredInputFormat = it.next();
			
			if ( anRegisteredInputFormat.isAssignableFrom(inputFormat) )
			{
				// the keys in the mapping have been sorted, the most
				// generic classes will be put at the end of the mapping
				// keys set, so the first input format class from the set
				// which is the equals or super class of the <code>inputFormat</code>
				// is the best helper.
				
				return this.mapping.get(anRegisteredInputFormat);
			}
		}
		
		throw new IllegalArgumentException("There is no "+MultiInputsHelper.class.getSimpleName()+" for handling "+inputFormat.getCanonicalName()+
				" please provide one using the register method in "+this.getClass().getSimpleName());
	}
	
	public MultiInputsHelpersRepository register(Class<? extends InputFormat> inputFormat, Class<? extends MultiInputsHelper> aFinder)
	{
		if( this.mapping.containsKey(inputFormat) )
		{
			MultiInputsHelper oldFinder = this.mapping.get(inputFormat);
			LOGGER.warn("Override the handler for ["+inputFormat.getCanonicalName()+"] from ["+oldFinder.getClass().getCanonicalName()+"]" +
					" to ["+aFinder.getClass().getCanonicalName()+"].");
		}
		
		this.mapping.put(inputFormat, ReflectionUtils.newInstance(aFinder, null));
		
		return this;
	}
	
	// TODO to be called before a job submission
	public void writeToConf(JobConf conf)
	{
		Iterator<Entry<Class<? extends InputFormat>, MultiInputsHelper>> entries 
			= this.mapping.entrySet().iterator();
		
		while( entries.hasNext() )
		{
			Entry<Class<? extends InputFormat>, MultiInputsHelper> anEntry = entries.next();
			Class<? extends InputFormat> inputFormat	= anEntry.getKey();
			Class<? extends MultiInputsHelper> helper	= anEntry.getValue().getClass();
			
			if ( !conf.get("mobius.multi.inputs.helpers", "").isEmpty() )
			{
				String others = conf.get("mobius.multi.inputs.helpers");
				conf.set("mobius.multi.inputs.helpers", others+","+inputFormat.getCanonicalName()+":"+helper.getCanonicalName());
			}
			else
			{
				conf.set("mobius.multi.inputs.helpers", inputFormat.getCanonicalName()+":"+helper.getCanonicalName());
			}
		}
	}
}
