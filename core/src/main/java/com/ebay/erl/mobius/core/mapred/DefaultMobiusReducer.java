package com.ebay.erl.mobius.core.mapred;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.ebay.erl.mobius.core.ConfigureConstants;
import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.collection.BigTupleList;
import com.ebay.erl.mobius.core.criterion.TupleCriterion;
import com.ebay.erl.mobius.core.datajoin.DataJoinReducer;
import com.ebay.erl.mobius.core.datajoin.DataJoinValueGroup;
import com.ebay.erl.mobius.core.function.base.ExtendFunction;
import com.ebay.erl.mobius.core.function.base.GroupFunction;
import com.ebay.erl.mobius.core.function.base.Projectable;
import com.ebay.erl.mobius.core.model.ReadFieldImpl;
import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.util.SerializableUtil;
import com.ebay.erl.mobius.util.Util;


/**
 * Reducer for handling Mobius joining and group 
 * by job. 
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
@SuppressWarnings({"deprecation", "unchecked"})
public class DefaultMobiusReducer extends DataJoinReducer<Tuple, Tuple, NullWritable, WritableComparable<?>>
{
	
private static final Log LOGGER = LogFactory.getLog(DefaultMobiusReducer.class);
	
	
	
	/**
	 * IDs of all the participated {@link Dataset}, in the order
	 * of left to right in a join job. 
	 * 
	 * If this is a join job, this array is used to keep track the
	 * current dataset is the expected one, if not, then we cannot
	 * perform inner join.
	 */
	private String[] _allDatasetIDs;
	
	/**
	 * array store all the dataset IDs but not the last
	 * one
	 */
	private String[] _allButNotLastDatasetIDs;
	
	
	
	/**
	 * A quick reference to get the last dataset ID.
	 */
	private String _lastDatasetID;
	
	
	
	/**
	 * Hadoop job config.
	 */
	private JobConf conf;
	
	
	
	/**
	 * the criteria specified by the user and to be applied 
	 * before the persistent step.
	 */
	protected TupleCriterion _persistantCriteria;
	
	
	
	/**
	 * the final projection functions.
	 */
	protected Projectable[] _projections = null; 
	
	
	
	/**
	 * The final projected column names, in user
	 * specified order.
	 */
	protected String[] outputColumnNames = null;
	
	
	
	/**
	 * A flag to indicate if we have set the reference
	 * of Hadoop reporter to every projectable functions
	 * or not.
	 */
	protected boolean reporterSet = false;
	
	
	
	/**
	 * When set to true, that mean there is at least one
	 * projectable function require columns from different
	 * datasets as the inputs.
	 */
	protected boolean requirePreCrossProduct = false;
	
	
	
	/**
	 * list of group functions that need columns from multiple
	 * datasets as the input.
	 */
	protected List<GroupFunction> multiDatasetGroupFunction = new LinkedList<GroupFunction>();
	
	
	
	/**
	 * list of extend functions that need columns from multiple
	 * datasets as the input.
	 */
	protected List<ExtendFunction> multiDatasetExtendFunction = null;
	
	
	
	/**
	 * mapping from a datasetID to a list of group functions that
	 * require columns only from that datasetID.
	 */
	protected Map<String, List<GroupFunction>> singleDatasetGroupFunction	= new HashMap<String, List<GroupFunction>>();
	
	
	
	/**
	 * mapping from a datasetID to a list of extend functions that
	 * require columns only from that datasetID.
	 */
	protected Map<String, List<ExtendFunction>> singleDatasetExtendFunction = new HashMap<String, List<ExtendFunction>>();
	
	
	
	/**
	 * mapping from a datasetID to the result of its extend functions
	 * that require only the columns from the dataset.
	 */
	protected Map<String, BigTupleList> singleDatasetExtendFunResult = new HashMap<String, BigTupleList>();
	
	
	
	/**
	 *  only used when <code>requirePreCrossProduct</code> is true.
	 */
	protected Map<String, BigTupleList> valuesForAllDatasets = new HashMap<String, BigTupleList>();
	
	
	
	/**
	 * A mapping to remember the schema of each dataset.
	 */
	private Map<String/*dataset ID*/, String[]/* schema belongs to the dataset*/> datasetToSchemaMapping 
		= new HashMap<String, String[]>();
		


	/**
	 * a boolean flag to indicate this job is outer join (
	 * including left-outer-join and right-outer-join) or 
	 * not.
	 */
	protected boolean isOuterJoin;
	
	/**
	 * the replacement specified by user to replace
	 * the null columns for outer-join job.
	 */
	protected Object nullReplacement;
	
	/**
	 * If the extend functions for a given dataset only
	 * require the join key, the engine will not compute
	 * it per values.
	 */
	private Map<String, Boolean> onlyHasGroupKeyExtendFunctions = new HashMap<String, Boolean>();
	
	@Override
	public void configure(JobConf conf)
	{
		super.configure(conf);
		this.conf = conf;
		
		///////////////////////////////////////////
		// setup the criteria to be applied in the
		// final projections
		///////////////////////////////////////////
		if( this.conf.get(ConfigureConstants.PERSISTANT_CRITERIA, null)!=null )
		{
			try 
			{
				this._persistantCriteria = (TupleCriterion)SerializableUtil.deserializeFromBase64(this.conf.get(ConfigureConstants.PERSISTANT_CRITERIA), this.conf);
			} 
			catch (IOException e) 
			{
				throw new IllegalArgumentException("Cannot deserialize "+ConfigureConstants.PERSISTANT_CRITERIA+
						" from ["+this.conf.get(ConfigureConstants.PERSISTANT_CRITERIA)+"]", e);
			}
		}
		
		////////////////////////////////////////
		// setup <code>_allDatasetIDs</code>
		////////////////////////////////////////
		this._allDatasetIDs = this.conf.getStrings(ConfigureConstants.ALL_DATASET_IDS, Util.ZERO_SIZE_STRING_ARRAY);
		if( this._allDatasetIDs.length==0 )
			throw new IllegalStateException(ConfigureConstants.ALL_DATASET_IDS+" is not set.");
		
		
		
		////////////////////////////////////////
		// setup <code>_lastDatasetID</code>
		////////////////////////////////////////
		this._lastDatasetID = this._allDatasetIDs[this._allDatasetIDs.length-1];
		
		this._allButNotLastDatasetIDs = new String[this._allDatasetIDs.length-1];
		for( int i=0;i<this._allDatasetIDs.length-1;i++ )
			this._allButNotLastDatasetIDs[i] = this._allDatasetIDs[i];
		
		
		///////////////////////////////////////
		// setup <code>_projections</code>
		//////////////////////////////////////
		try 
		{
			this._projections = (Projectable[]) SerializableUtil.deserializeFromBase64(this.conf.get(ConfigureConstants.PROJECTION_COLUMNS), this.conf);
			List<String> outptuColumnNames = new ArrayList<String>();
			for(Projectable p:this._projections )
			{
				p.setCalledByCombiner(false);
				// save the output columns in user specified order
				// so that the tuples in the final projections
				// can emit the columns in user expected ordering.
				for( String name:p.getOutputSchema() )
					outptuColumnNames.add(name);
			}
			this.outputColumnNames = outptuColumnNames.toArray(new String[outptuColumnNames.size()]);
		} 
		catch (IOException e) 
		{	
			throw new IllegalArgumentException(e);
		}
		
		
		// use then in final cross-product.
		
		for( Projectable func: this._projections )
		{
			if( func.requireDataFromMultiDatasets() )
			{
				// there is at least one projectable function require columns
				// from different datasets, these functions require cross-product
				// the values from different datasets to compute their values,
				// we need to set this flag to true so later we will do the 
				// cross product.
				requirePreCrossProduct = true;
				
				if( func instanceof GroupFunction )
				{
					if( this.multiDatasetGroupFunction==null )
						this.multiDatasetGroupFunction = new LinkedList<GroupFunction>();
					this.multiDatasetGroupFunction.add((GroupFunction)func);
				}
				else if ( func instanceof ExtendFunction )
				{
					if( this.multiDatasetExtendFunction==null )
						this.multiDatasetExtendFunction = new LinkedList<ExtendFunction>();
					this.multiDatasetExtendFunction.add((ExtendFunction)func);
				}
				else
				{
					throw new IllegalArgumentException(func.getClass().getCanonicalName()+" is not a sub-class of "+
							GroupFunction.class.getCanonicalName()+" nor, "+
							ExtendFunction.class.getCanonicalName());
				}
			}
			else
			{
				// projectable functions that require columns from one dataset only.
				
				boolean onlyUseGroupKey = true;
				
				String datasetID = this.getDatasetID(func.getParticipatedDataset().toArray(new Dataset[0])[0]);
				if( func instanceof GroupFunction )
				{
					List<GroupFunction> funcs = null;
					if( (funcs=this.singleDatasetGroupFunction.get(datasetID))==null ){
						funcs = new LinkedList<GroupFunction>();
						this.singleDatasetGroupFunction.put(datasetID, funcs);
					}
					funcs.add((GroupFunction)func);
				}
				else if ( func instanceof ExtendFunction )
				{
					List<ExtendFunction> funcs = null;
					if( (funcs=this.singleDatasetExtendFunction.get(datasetID))==null ){
						funcs = new LinkedList<ExtendFunction>();
						this.singleDatasetExtendFunction.put(datasetID, funcs);
					}
					funcs.add((ExtendFunction)func);
					if( !func.useGroupKeyOnly() )
						onlyUseGroupKey = false;
				}
				else
				{
					throw new IllegalArgumentException(func.getClass().getCanonicalName()+" is not a sub-class of "+
							GroupFunction.class.getCanonicalName()+" nor, "+
							ExtendFunction.class.getCanonicalName());
				}
				
				this.onlyHasGroupKeyExtendFunctions.put(datasetID, onlyUseGroupKey);
			}
		}
		
		
		this.isOuterJoin = this.conf.getBoolean(ConfigureConstants.IS_OUTER_JOIN, false);
		
		
		////////////////////////////////////////
		// setup <code>nullReplacement</code>
		////////////////////////////////////////
		try
		{
			if( this.conf.get(ConfigureConstants.NULL_REPLACEMENT, null)!=null )
			{
				
				byte[] binary 	= Base64.decodeBase64(this.conf.get(ConfigureConstants.NULL_REPLACEMENT).getBytes("UTF-8"));			
				byte type		= (byte)this.conf.getInt(ConfigureConstants.NULL_REPLACEMENT_TYPE, -1);
				
				ByteArrayInputStream buffer = new ByteArrayInputStream(binary);
				DataInputStream input		= new DataInputStream(buffer);
				
				List<Object> temp			= new LinkedList<Object>();
				ReadFieldImpl reader 		= new ReadFieldImpl(temp, input, this.conf);
				reader.handle(type);
				
				this.nullReplacement		= temp.remove(0);
			}
		}
		catch(IOException e)
		{
			throw new RuntimeException("Cannot deserialize null_replacement from:" +
					"["+this.conf.get(ConfigureConstants.NULL_REPLACEMENT)+"]", e);
		}
		
	}
	
	
	@Override
	public void joinreduce(Tuple key, DataJoinValueGroup<Tuple> values, OutputCollector<NullWritable, WritableComparable<?>> output, Reporter reporter) 
		throws IOException 
	{	
		// set reporter for all projectable
		if( !reporterSet )
		{			
			for( Projectable p:this._projections )
			{
				LOGGER.info("Set reporter to "+p.getClass().getCanonicalName());
				p.setReporter(reporter);
			}
			reporterSet = true;
		}		
		
		this.clearPreviousResults();		
		
		int expectingDatasetIDX = 0;		
		// don't keep the values from last dataset into {@BigTupleList},
		// using iterator to iterate them through to perform
		// cross product
		Iterator<Tuple> valuesFromLastDataset = null;
		

		
		//////////////////////////////////////////
		// compute the result for the projections
		//////////////////////////////////////////
		while( values.hasNext() )
		{
			String datasetID = values.nextDatasetID ();		
			
			if ( !datasetID.equals (_allDatasetIDs[expectingDatasetIDX]) )
			{
				// no records coming from the expected dataset, means
				// 1) not full inner join-able, 
				// 2) no records from the left dataset when this is a left-outer-join, or 
				// 3) no records from the right dataset when this is a right-outer-join, return
				return;
			}
			
			expectingDatasetIDX++;
			if( !datasetID.equals(this._lastDatasetID) )// values not from the last dataset
			{
				Iterator<Tuple> valuesForCurrentDataset = values.next ();
				computeSingleDSFunctionsResults(valuesForCurrentDataset, datasetID, reporter);
			}
			else
			{
				// the remaining values are all from the last
				// dataset, keep the reference of the value
				// iterator to perform cross product later
				valuesFromLastDataset = values.next();
				break;
			}
		}
		
		if ( valuesFromLastDataset == null )
		{
			if( !this.isOuterJoin )
			{
				// no records from the last dataset, not be able to
				// do full inner join, return
				return;
			}
			else
			{
				// no records from the last dataset, but this is a 
				// outer-join job, continue.
			}
		}
		
		
			
		//////////////////////////////////////////////////////////////
		// cross product the results for all the datasets except
		// the last one, the result only contain projectable functions
		// that don't require columns from multiple datasets only.
		/////////////////////////////////////////////////////////////
		Iterable<Tuple> resultsFromOtherDatasets = this.crossProduct(reporter, false, _allButNotLastDatasetIDs);
		
		List<Iterable<Tuple> > toBeCrossProduct = new ArrayList<Iterable<Tuple>>();
		if( resultsFromOtherDatasets!=null)
			toBeCrossProduct.add(resultsFromOtherDatasets);
		
		
		
		/////////////////////////////////////////////////////
		// start to compute the results for the final dataset
		/////////////////////////////////////////////////////
		
		
		
		boolean hasMultiDSFunctions = this.requirePreCrossProduct;
		
		if( hasMultiDSFunctions )
		{
			// there are functions require columns from multiple
			// dataset, so save the values from last dataset
			// into BigTupleList so we can iterate it multiple
			// times.
			if( valuesFromLastDataset!=null )
			{
				while( valuesFromLastDataset.hasNext() )
				{
					Tuple aRow = valuesFromLastDataset.next();
					this.rememberTuple(_lastDatasetID, aRow, reporter);
				}
				
				Iterable<Tuple> preCrossProduct = Util.crossProduct(conf, reporter, this.valuesForAllDatasets.values().toArray(new BigTupleList[0]));
				BigTupleList btl = new BigTupleList(reporter);
				for( Tuple aRow:preCrossProduct )
				{
					this.computeExtendFunctions(aRow, btl, this.multiDatasetExtendFunction);
					this.computeGroupFunctions(aRow, this.multiDatasetGroupFunction);
				}
				
				if( btl.size()>0 )
					toBeCrossProduct.add(btl);
				for(GroupFunction fun:this.multiDatasetGroupFunction )
					toBeCrossProduct.add(fun.getResult());
				
				valuesFromLastDataset = this.valuesForAllDatasets.get(_lastDatasetID).iterator();
			}
			else
			{
				if( this.multiDatasetExtendFunction.size()>0 )
				{
					BigTupleList btl = new BigTupleList(reporter);
					this.computeExtendFunctions(null, btl, this.multiDatasetExtendFunction);
					toBeCrossProduct.add(btl);
				}
				for(GroupFunction fun:this.multiDatasetGroupFunction )
					toBeCrossProduct.add(fun.getNoMatchResult(nullReplacement));
			}
		}
		// finished the computation of multi-dataset functions, start
		// to compute the projectable funcitons results for last
		// dataset
		//
		// first compute the cross product of all other functions
		Iterable<Tuple> others = null;
		if( toBeCrossProduct.size()>0 )
		{
			Iterable<Tuple>[] array = new Iterable[toBeCrossProduct.size()];
			for( int i=0;i<toBeCrossProduct.size();i++ )
			{
				array[i] = toBeCrossProduct.get(i);
			}
			
			others = Util.crossProduct(conf, reporter, array);
		}
		
		if( valuesFromLastDataset==null )
		{// outer-join, so <code>others</code> is always not null.
			List<BigTupleList> nullResult = new ArrayList<BigTupleList>();
			
			if( this.singleDatasetExtendFunction.get(_lastDatasetID)!=null )
			{
				BigTupleList btl = new BigTupleList(reporter);
				this.computeExtendFunctions(null, btl, this.singleDatasetExtendFunction.get(_lastDatasetID));
				nullResult.add(btl);
			}
			if( this.singleDatasetGroupFunction.get(_lastDatasetID)!=null )
			{
				for(GroupFunction fun:this.singleDatasetGroupFunction.get(_lastDatasetID) )
					nullResult.add(fun.getNoMatchResult(nullReplacement));
			}
			
			for( Tuple t1:Util.crossProduct(conf, reporter, nullResult) )
			{
				for( Tuple t2:others )
				{
					this.output(Tuple.merge(t1, t2), output, reporter);
				}
			}
		}
		else
		{
			boolean hasNoGroupFunctionForLastDS = this.singleDatasetGroupFunction.get(this._lastDatasetID)==null;
			while( valuesFromLastDataset.hasNext() )
			{
				Tuple aRow = valuesFromLastDataset.next();
				aRow.setSchema(this.getSchemaByDatasetID(_lastDatasetID));
				if(hasNoGroupFunctionForLastDS)
				{
					// there is no group function from the last DS, we can
					// do some optimization here: as we streaming over the
					// values of last dataset, we also emit outputs.
				
					
					Tuple merged = new Tuple();
					for( ExtendFunction func:this.singleDatasetExtendFunction.get(_lastDatasetID) )
					{
						merged = Tuple.merge(merged, func.getResult(aRow));
					}
					
					if( others!=null )
					{
						for(Tuple t:others)
						{
							this.output(Tuple.merge(t, merged), output, reporter);
						}
					}
					else
					{
						this.output(merged, output, reporter);
					}
				}
				else
				{
					this.processExtendFunctions(_lastDatasetID, aRow, reporter);
					this.computeGroupFunctions(_lastDatasetID, aRow);
				}
			}
			
			if(!hasNoGroupFunctionForLastDS)
			{
				for( Tuple t1: this.crossProduct(reporter, false, _lastDatasetID) )
				{
					if( others!=null )
					{
						for(Tuple t2:others)
						{
							this.output(Tuple.merge(t1, t2), output, reporter);
						}
					}
					else
					{
						this.output(t1, output, reporter);
					}
				}
			}
		}
	}
	
	/**
	 * compute functions that require columns from the datasetID only.
	 */
	private void computeSingleDSFunctionsResults(Iterator<Tuple> tuples, String datasetID, Reporter reporter)
	{
		while( tuples.hasNext() )
		{
			Tuple aTuple = tuples.next();
			aTuple.setSchema(this.getSchemaByDatasetID(datasetID));
			
			if( this.requirePreCrossProduct )
			{
				// some functions need columns from multiple
				// dataset, so remember this value for later
				// corss product
				rememberTuple(datasetID, aTuple, reporter);
			}
			this.processExtendFunctions(datasetID, aTuple, reporter);
			this.computeGroupFunctions(datasetID, aTuple);
		}
	}
	
	
	private void output(Tuple aTuple, OutputCollector<NullWritable, WritableComparable<?>> output, Reporter reporter)
		throws IOException
	{
		aTuple.setToStringOrdering(this.outputColumnNames);
		if( this._persistantCriteria!=null )
		{
			if( this._persistantCriteria.accept(aTuple, this.conf) )
			{
				output.collect(NullWritable.get(), aTuple);
				reporter.getCounter("Join/Grouping Records", "EMITTED").increment(1);
			}
			else
			{
				reporter.getCounter("Join/Grouping Records", "FILTERED").increment(1);
			}
		}
		else
		{
			output.collect(NullWritable.get(), aTuple);
			reporter.getCounter("Join/Grouping Records", "EMITTED").increment(1);
		}
	}
	
	
	private void rememberTuple(String datasetID, Tuple aTuple, Reporter reporter){
		BigTupleList tuples = null;
		if( (tuples=this.valuesForAllDatasets.get(datasetID))==null )
		{
			tuples = new BigTupleList(reporter);
			this.valuesForAllDatasets.put(datasetID, tuples);
		}
		tuples.add(aTuple);
	}
	
	
	/**
	 * compute the extend functions for the given datasetID, using the 
	 * <code>aRow</code> as the input and save the result for final
	 * cross-product.
	 */
	private void processExtendFunctions(String datasetID, Tuple aRow, Reporter reporter)
	{
		// process extend function for this current dataset and save the result
		List<ExtendFunction> extendFunctions	= this.singleDatasetExtendFunction.get(datasetID);
		if( extendFunctions==null )
			return;
		
		BigTupleList computedResult				= null;
		if( (computedResult=this.singleDatasetExtendFunResult.get(datasetID))==null )
		{
			computedResult = new BigTupleList(reporter);
			this.singleDatasetExtendFunResult.put(datasetID, computedResult);
		}
		if( onlyHasGroupKeyExtendFunctions.get(datasetID) )
		{
			if(computedResult.size()==0 )
				this.computeExtendFunctions(aRow, computedResult, extendFunctions);
		}
		else
		{
			this.computeExtendFunctions(aRow, computedResult, extendFunctions);
		}
	}
	
	/**
	 * compute the extend function result using the <code>aRow</code>,
	 * merge the tuple from each function together into single one and
	 * add it to the <code>result</code> list for final cross-product.
	 */
	private void computeExtendFunctions(Tuple aRow, BigTupleList result, List<ExtendFunction> functions)
	{
		if( functions!=null && !functions.isEmpty() )
		{
			Tuple mergedResult = new Tuple();
			for( ExtendFunction aFunction:functions )
			{
				if( aRow!=null )
					mergedResult = Tuple.merge(mergedResult, aFunction.getResult(aRow));
				else
					mergedResult = Tuple.merge(mergedResult, aFunction.getNoMatchResult(nullReplacement));
			}
			result.add(mergedResult);
		}
	}
	
	/**
	 * For each group function from of the given datasetID,
	 * call their consume method with the <code>aRow</code>
	 * as the input.
	 */
	private void computeGroupFunctions(String datasetID, Tuple aRow)
	{
		List<GroupFunction> groupFunctions = this.singleDatasetGroupFunction.get(datasetID);
		this.computeGroupFunctions(aRow, groupFunctions);
	}
	private void computeGroupFunctions(Tuple aRow, List<GroupFunction> functions)
	{
		if( functions!=null && !functions.isEmpty()){
			for( GroupFunction aFunction:functions )
			{
				if( aRow!=null)
					aFunction.consume(aRow);
			}
		}
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
	
	
	private void clearPreviousResults()
	{
		for( BigTupleList list:this.singleDatasetExtendFunResult.values() )
		{
			list.clear();
		}
		
		for( Projectable fun:this._projections ){
			if( fun instanceof GroupFunction ){
				((GroupFunction)fun).reset();
			}
		}
		for( String aDataset:this.valuesForAllDatasets.keySet() )
		{
			this.valuesForAllDatasets.remove(aDataset).clear();
		}
		
	}
	
	protected String[] getSchemaByDatasetID(String datasetID)
	{
		String[] schema = null;
		if( (schema=this.datasetToSchemaMapping.get(datasetID))==null )
		{
			schema = this.conf.getStrings(datasetID+".value.columns", Util.ZERO_SIZE_STRING_ARRAY);
			if( schema.length==0 )
			{
				// should never happen
				throw new IllegalStateException("Schema for dataset:"+datasetID+" is not set.");
			}
			
			this.datasetToSchemaMapping.put(datasetID, schema);
		}
		return schema;
	}
	
	
	
	/**
	 * compute the cross product result of the given dataset id
	 */
	private Iterable<Tuple> crossProduct(Reporter reporter, boolean usingNull, String... datasetIDs)
		throws IOException
	{
		
		if( datasetIDs==null || datasetIDs.length==0 )
			return null;
		
		List<BigTupleList> resultsToBeCrossProducts = new ArrayList<BigTupleList>();
		for( String datasetID:datasetIDs )
		{
			if( this.singleDatasetExtendFunResult.get(datasetID)!=null )
			{
				resultsToBeCrossProducts.add(this.singleDatasetExtendFunResult.get(datasetID));
			}
				
			if( this.singleDatasetGroupFunction.get(datasetID)!=null )
			{
				for( GroupFunction fun:this.singleDatasetGroupFunction.get(datasetID) )
				{
					if( usingNull )
						resultsToBeCrossProducts.add(fun.getNoMatchResult(this.nullReplacement));
					else
						resultsToBeCrossProducts.add(fun.getResult());
				}
			}
		}
		
		return Util.crossProduct(conf, reporter, resultsToBeCrossProducts);
	}
}
