package com.ebay.erl.mobius.core.mapred;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

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
@SuppressWarnings({"deprecation"})
public class FileInputFormatHelper extends MultiInputsHelper
{

	private static final long serialVersionUID = -6882265957350324331L;
	private transient FileSystem fs;
	
	private FileSystem getFileSystem(JobConf conf)
		throws IOException
	{
		if( this.fs==null )
		{
			this.fs = FileSystem.get(conf);
		}
		return this.fs;
	}

	@Override
	public URI getUniquePathByInputFormat(JobConf conf, Path anInput) 
		throws IOException
	{
		// since it's FileInputFormat, the ID can be represented just 
		// using the input path
		
		
		Path result = this.getFileSystem(conf).makeQualified(anInput);
		
		if( !this.getFileSystem(conf).isFile(anInput) && result.toUri().getPath().endsWith("/") )
		{
			// the given input is a folder but it's path string doesn't
			// end with slash, then add it can be distinguished by
			// just it's string representation.
			
			result = new Path(result.toString()+"/");
		}
		
		return this.getFileSystem(conf).makeQualified(result).toUri();
	}

	@Override
	public URI getPathBySplit(InputSplit split, JobConf conf) 
		throws IOException
	{
		if( split instanceof FileSplit )
		{
			FileSplit fileSplit = (FileSplit)split;			
			return this.getFileSystem(conf).makeQualified(fileSplit.getPath()).toUri();
		}
		return null;
	}

}
