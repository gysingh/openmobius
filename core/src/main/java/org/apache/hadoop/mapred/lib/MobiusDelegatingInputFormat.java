package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.ebay.erl.mobius.core.ConfigureConstants;
import com.ebay.erl.mobius.core.mapred.AbstractMobiusMapper;
import com.ebay.erl.mobius.core.mapred.MultiInputsHelpersRepository;

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
 * @param <K>
 * @param <V>
 */
@SuppressWarnings({ "deprecation" })
public class MobiusDelegatingInputFormat <K, V> extends DelegatingInputFormat<K, V>
{	
	private Map<URI, String> _URI_TO_DATASETID_MAPPING;
	
	private List<URI> _INPUT_URIS;
	
	private final String _INIT_KEY = "";
	
	private static final Log LOGGER = LogFactory.getLog(MobiusDelegatingInputFormat.class);
	
	// getting the mapper which can process the input split
	public Class<AbstractMobiusMapper> getMapper(InputSplit split, JobConf conf)
		throws IOException
	{
		TaggedInputSplit taggedSplit	= (TaggedInputSplit)split;
		InputSplit inputSplit			= taggedSplit.getInputSplit();
		URI currentFileURI	= MultiInputsHelpersRepository.getInstance(conf).getURIBySplit(inputSplit, conf);
		
		try
		{
			String[] pathToMapperMappings = conf.get("mapred.input.dir.mappers").split(",");
			for( String aPathToMapper:pathToMapperMappings)
			{
				//System.out.println("aPathToMapper:"+aPathToMapper);
				//System.out.println("currentFileURI:"+currentFileURI.toString());
				
				String[] data = aPathToMapper.split(";");
				URI path = new URI(data[0]);
				URI relative = path.relativize(currentFileURI);
				
				//System.out.println("relative:"+relative);
				
				
				String mapperClassName = data[1];
				if( currentFileURI.equals(path) || !relative.equals(currentFileURI) )
				{
					return (Class<AbstractMobiusMapper>)Class.forName(mapperClassName);
				}
			}
		}catch(Exception e)
		{
			throw new RuntimeException(e);
		}
		return null;
	}
	

	@Override
	public RecordReader<K, V> getRecordReader(InputSplit split, JobConf conf, Reporter reporter) throws IOException
	{	
		this.setupLookupTables(conf);
		
		String datasetID = getDatasetIDBySplit(split, conf);
		
		conf.set (ConfigureConstants.CURRENT_DATASET_ID, datasetID);
		RecordReader<K, V> reader = super.getRecordReader (split, conf, reporter);
		return reader;		
	}
	
	
	private String getDatasetIDBySplit(InputSplit split, JobConf conf)
		throws IOException
	{
		// The <code>split</code> is an instance of {@link TaggedInputSplit}
		// but the TaggedInputSplit is not a public class, so we need to place
		// this class under the package of org.apache.hadoop.mapred.lib.
		
		TaggedInputSplit taggedSplit	= (TaggedInputSplit)split;
		InputSplit inputSplit			= taggedSplit.getInputSplit();		
		URI currentFileURI				= MultiInputsHelpersRepository.getInstance(conf).getURIBySplit(inputSplit, conf);
		String currentFile				= currentFileURI.toString();
		
		
		LOGGER.debug("Using ["+currentFile+"] to locate current Dataset");
		
		String datasetID = null;
		for( URI anInput:_INPUT_URIS )
		{
			if( anInput.equals(currentFileURI) )
			{
				datasetID = _URI_TO_DATASETID_MAPPING.get (anInput);
				if ( datasetID == null || datasetID.trim ().length () == 0 )
					throw new IllegalArgumentException ("Dataet ID for the input path:[" + anInput+ "] did not set.");
			}
			else
			{
				// not equal, compute the relative URI
				URI relative = anInput.relativize(currentFileURI);
				if( !relative.equals(currentFileURI) )
				{
					// found the key
					datasetID = _URI_TO_DATASETID_MAPPING.get (anInput);
					if ( datasetID == null || datasetID.trim ().length () == 0 )
						throw new IllegalArgumentException ("Dataet ID for the input path:[" + anInput+ "] did not set.");
				}
			}
		}
		
		if( datasetID==null )
		{
			throw new IllegalArgumentException ("Cannot find dataset id using the given uri:[" + currentFile + "], " + 
						ConfigureConstants.INPUT_TO_DATASET_MAPPING+":" + conf.get (ConfigureConstants.INPUT_TO_DATASET_MAPPING));
		}
		
		return datasetID;
	}
	
	private void setupLookupTables(JobConf conf)
	{
		// due to this bug: https://issues.apache.org/jira/browse/MAPREDUCE-1743
		// map.input.file is not set when using MultipleInputs, which is used in 
		// {@link MobiusMultiInputs}, we need to set it.
		synchronized(_INIT_KEY)
		{	
			_URI_TO_DATASETID_MAPPING	= new TreeMap<URI, String>();
			_INPUT_URIS					= new ArrayList<URI>();
				
			// in the format of datasetID;input_uri(,datasetID;input_uri)*
			String[] mappings = conf.getStrings (ConfigureConstants.INPUT_TO_DATASET_MAPPING);
			for(String aMapping:mappings )
			{
				String[] data 		= aMapping.split(";");
				String datasetID 	= data[0];
				String input_uri 	= data[1];
					
				try 
				{
					URI anInput 	= new URI(input_uri);
					_INPUT_URIS.add(anInput);
					_URI_TO_DATASETID_MAPPING.put(anInput, datasetID);
				}
				catch (URISyntaxException e) 
				{
					throw new RuntimeException(e);
				}
			}
		}
			
		Collections.sort(_INPUT_URIS);
		// reverse the order so the system can check the URI from most specific to
		// less specific
		Collections.reverse(_INPUT_URIS);
	}
}
