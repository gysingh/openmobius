package com.ebay.erl.mobius.core.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.ebay.erl.mobius.core.ConfigureConstants;
import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.collection.BigTupleList;
import com.ebay.erl.mobius.core.datajoin.DataJoinKey;
import com.ebay.erl.mobius.core.datajoin.DataJoinReducer;
import com.ebay.erl.mobius.core.datajoin.DataJoinValue;
import com.ebay.erl.mobius.core.datajoin.DataJoinValueGroup;
import com.ebay.erl.mobius.core.function.base.ExtendFunction;
import com.ebay.erl.mobius.core.function.base.GroupFunction;
import com.ebay.erl.mobius.core.function.base.Projectable;
import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.util.SerializableUtil;
import com.ebay.erl.mobius.util.Util;

/**
 * Default combiner for join or group-by job if
 * all the projectable columns are combinable,
 * determined by {@link Projectable#isCombinable()}.
 * 
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
@SuppressWarnings("deprecation")
public class DefaultMobiusCombiner extends DataJoinReducer<Tuple, Tuple, DataJoinKey, DataJoinValue>
{
	protected Projectable[] _projections = null;
	
	private String[] _allDatasetIDs;
	
	private JobConf conf;
	
	private Map<String, String[]> datasetToValueSchemaMapping = new HashMap<String, String[]>();
	
	private Map<String, String[]> datasetToKeySchemaMapping = new HashMap<String, String[]>();
	
	private Map<GroupFunction, BigTupleList> groupFunctionResults = new HashMap<GroupFunction, BigTupleList>();
	
	private Map<String, List<Projectable> > dsToFuncsMapping = new HashMap<String, List<Projectable>>();
	
	private boolean reporterSet = false;
	
	@Override
	public void configure(JobConf conf)
	{
		super.configure(conf);
		this.conf = conf;
		try 
		{
			this._allDatasetIDs = this.conf.getStrings(ConfigureConstants.ALL_DATASET_IDS, Util.ZERO_SIZE_STRING_ARRAY);
			if( this._allDatasetIDs.length==0 )
				throw new IllegalStateException(ConfigureConstants.ALL_DATASET_IDS+" is not set.");
			
			this._projections = (Projectable[]) SerializableUtil.deserializeFromBase64(this.conf.get(ConfigureConstants.PROJECTION_COLUMNS), this.conf);
			for( Projectable p:this._projections )
			{
				if( !p.isCombinable() )
				{
					throw new IllegalArgumentException(p.toString()+" is not a combinable function.");
				}
				
				String datasetID = this.getDatasetID(p.getParticipatedDataset().toArray(new Dataset[0])[0]);
				
				List<Projectable> funcs = null;
				if( (funcs=dsToFuncsMapping.get(datasetID))==null )
				{
					funcs = new ArrayList<Projectable>();
					dsToFuncsMapping.put(datasetID, funcs);
				}
				funcs.add(p);
				
				p.setCalledByCombiner(true);
				
				if( p instanceof GroupFunction )
				{
					groupFunctionResults.put((GroupFunction)p, new BigTupleList(null));
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
	}

	@Override
	public void joinreduce(Tuple key, DataJoinValueGroup<Tuple> values, OutputCollector<DataJoinKey, DataJoinValue> output, Reporter reporter)
		throws IOException 
	{
		if( !reporterSet )
		{
			for(Projectable p:this._projections )
			{
				p.setReporter(reporter);
			}
			reporterSet = true;
		}
		
		
		if( values.hasNext () )
		{
			// reset group function results.
			if( groupFunctionResults.size()>0 )
			{
				for(GroupFunction func:this.groupFunctionResults.keySet() )
				{
					this.groupFunctionResults.get(func).clear();
					func.reset();
				}
			}
			
			
			String datasetID = values.nextDatasetID ();
			
			if( !key.hasSchema() )
			{
				key.setSchema(this.getKeySchemaByDatasetID(datasetID));
			}
			
			Iterator<Tuple> tuples = values.next ();
			
			Tuple combinedValue = new Tuple();
			
			while( tuples.hasNext() )
			{
				Tuple aTuple = tuples.next();
				aTuple.setSchema(this.getValueSchemaByDatasetID(datasetID));
				
				for( Projectable p:this.dsToFuncsMapping.get(datasetID) )
				{
					if( p instanceof GroupFunction )
					{
						((GroupFunction) p).consume(aTuple);
					}
					else
					{
						ExtendFunction func		= (ExtendFunction)p;
						Tuple computedResult 	= func.getResult(aTuple);
						
						String name = func.getInputColumns()[0].getInputColumnName();						
						combinedValue.insert(name, computedResult.get(0));
					}
				}
			}
			
			for( Projectable p:this.dsToFuncsMapping.get(datasetID) )
			{
				if( p instanceof GroupFunction )
				{
					BigTupleList aggregatedResult = ((GroupFunction)p).getResult();
					if( aggregatedResult.size() ==1 )
					{
						Tuple aggResult = aggregatedResult.getFirst();
						String name = p.getInputColumns()[0].getInputColumnName();
						combinedValue.insert(name, aggResult.get(0));
					}
					else if( aggregatedResult.size()>1 )
						throw new IllegalArgumentException(p.toString()+" is a group function that generates " +
								"more than one rows ("+aggregatedResult.size()+") per key, so it is not combinable.");
				}
			}
			
			output.collect(new DataJoinKey(datasetID, key), new DataJoinValue(datasetID, combinedValue));
		}
	}
	
	
	protected String[] getValueSchemaByDatasetID(String datasetID)
	{
		String[] schema = null;
		if( (schema=this.datasetToValueSchemaMapping.get(datasetID))==null )
		{
			schema = this.conf.getStrings(datasetID+".value.columns", Util.ZERO_SIZE_STRING_ARRAY);
			if( schema.length==0 )
			{
				// should never happen
				throw new IllegalStateException("Schema for dataset:"+datasetID+" is not set.");
			}
			
			this.datasetToValueSchemaMapping.put(datasetID, schema);
		}
		return schema;
	}
	
	protected String[] getKeySchemaByDatasetID(String datasetID)
	{
		String[] schema = null;
		if( (schema=this.datasetToKeySchemaMapping.get(datasetID))==null )
		{
			schema = this.conf.getStrings(datasetID+".key.columns", Util.ZERO_SIZE_STRING_ARRAY);
			if( schema.length==0 )
			{
				// should never happen
				throw new IllegalStateException("Schema for dataset:"+datasetID+" is not set.");
			}
			
			this.datasetToKeySchemaMapping.put(datasetID, schema);
		}
		return schema;
	}

	
	private String getDatasetID(Dataset ds){		
		// use the ds#getName() to match its ID in the
		// <code>_allDatasetIDs</code> array.
		String key = null;
		for(String aDatasetID:this._allDatasetIDs)
		{
			String name = aDatasetID.substring(aDatasetID.indexOf("_")+1);// remove sn prefix
			if( name.equalsIgnoreCase(ds.getName()) )
			{
				key = aDatasetID;
				break;
			}
		}
		if( key==null )
			throw new IllegalArgumentException("Cannot find the ID for Dataset:"+ds);
		return key;
	}
}
