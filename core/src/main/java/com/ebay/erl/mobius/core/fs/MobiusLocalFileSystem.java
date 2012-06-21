package com.ebay.erl.mobius.core.fs;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

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
 */
public class MobiusLocalFileSystem extends LocalFileSystem
{
	public MobiusLocalFileSystem()
	{
		super(new RawLocalFileSystem());
	}
	
	public boolean mkdirs(Path f, FsPermission permission) throws IOException 
	{
		URI uri = f.toUri();
		
		File file = new File(uri.getPath());
		
		if( !file.exists() )
		{
			boolean b = file.mkdirs();
			if( !b )
				throw new IOException(file.getAbsolutePath());
			return b;
		}else
		{
			return true;
		}
	}
	
	public boolean mkdirs(Path f) throws IOException 
	{
	    return mkdirs(f, FsPermission.getDefault());
	}
	
	public void setPermission(Path p, FsPermission permission) throws IOException 
	{
		// do nothing.
	}
	
	
	public static boolean mkdirs(FileSystem fs, Path dir, FsPermission permission) throws IOException 
	{
		
		// create the directory using the default permission
		boolean result = fs.mkdirs(dir);
		// set its permission to be the supplied one
		//fs.setPermission(dir, permission);
		return result;
	}
}
