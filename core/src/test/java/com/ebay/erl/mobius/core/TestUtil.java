package com.ebay.erl.mobius.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

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
public class TestUtil 
{
	public static boolean equalFile(File generatedFileFolder, File trueAnswerFile)
		throws IOException
	{
		StringBuffer generatedContent	= new StringBuffer();
		StringBuffer trueAnswerContent	= new StringBuffer();
		for( File aFile:generatedFileFolder.listFiles() )
		{
			if( aFile.getName().startsWith("part") )
			{
				BufferedReader br = new BufferedReader(new FileReader(aFile));
				String newLine = null;
				while( (newLine=br.readLine())!=null )
				{
					generatedContent.append(newLine);
				}
				br.close();
			}
		}
		
		BufferedReader br = new BufferedReader(new FileReader(trueAnswerFile));
		String newLine = null;
		while( (newLine=br.readLine())!=null )
		{
			trueAnswerContent.append(newLine);
		}
		br.close();
		
		return generatedContent.toString().trim().equals(trueAnswerContent.toString().trim());
	}
}
