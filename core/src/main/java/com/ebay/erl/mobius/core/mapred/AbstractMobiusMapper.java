package com.ebay.erl.mobius.core.mapred;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.IllegalFormatException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.ebay.erl.mobius.core.ConfigureConstants;
import com.ebay.erl.mobius.core.collection.BigTupleList;
import com.ebay.erl.mobius.core.criterion.TupleCriterion;
import com.ebay.erl.mobius.core.datajoin.DataJoinMapper;
import com.ebay.erl.mobius.core.model.ComputedColumns;
import com.ebay.erl.mobius.core.model.KeyTuple;
import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.util.SerializableUtil;
import com.ebay.erl.mobius.util.Util;

/**
 * Base class for implementing a customized Mobius mapper.
 * <p>
 * 
 * Extends this class if the built-in mappers, 
 * {@link com.ebay.erl.mobius.core.mapred.TSVMapper} and 
 * {@link com.ebay.erl.mobius.core.mapred.SequenceFileMapper}, 
 * does not meet the needs.
 * <p>
 * 
 * This class provides filtering (by taking user specified
 * {@link #tuple_criteria}), compute {@link #computedColumns},
 * and updating counters.
 * <p>
 * 
 * Override the {@link #parse(Object, Object)} method to convert
 * the K-V objects into a tuple, then the underlying data source
 * can be processed by mobius.
 * <p> 
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
 *
 * @param <IK> input key type.
 * @param <IV> input value type.
 * 
 */
@SuppressWarnings("deprecation")
public abstract class AbstractMobiusMapper<IK, IV> extends DataJoinMapper<IK, IV, WritableComparable<?>, WritableComparable<?>>
{
	
	/**
	 * filters
	 */
	protected TupleCriterion tuple_criteria;
	
	/**
	 * columns to be emitted as key of this {@link Mapper}
	 */
	protected String[] key_columns;
	
	/**
	 * columns to be emitted as value of this {@link Mapper}
	 */
	protected String[] value_columns;
	
	/**
	 * Output column names for map only job, ex: listing.
	 */
	protected String[] projection_order;
	
	
	/**
	 * The current dataset ID.
	 */
	protected String currentDatasetID = null;
	
	
	/**
	 * The normalized name of the dataset been processed by this 
	 * mapper currently, it is used as counter ID to update the 
	 * corresponding Hadoop counters for this dataset.
	 * <p>
	 * 
	 * The name is normalized from the {@link #currentDatasetID} by
	 * removing the serial number part.
	 */
	protected String dataset_display_id;
	
	/**
	 * A background thread responsible for updating the 
	 * Hadoop counters.
	 */
	protected CounterUpdateThread counterThread;
	
	/**
	 * Counts for the number of input records.
	 * <p>
	 * 
	 * #INPUT_RECORDS = #FILTERED_RECORDS + #OUTPUT_RECORDS.
	 */
	protected long _COUNTER_INPUT_RECORD;
	
	/**
	 * Counts for the number of outputted records.
	 */
	protected long _COUNTER_OUTPUT_RECORD;
	
	/**
	 * Counts for the number of filtered records,
	 * filtered by user specified {@link #tuple_criteria}.
	 */
	protected long _COUNTER_FILTERED_RECORD;
	
	/**
	 * Counts for invalidate format records.
	 */
	protected long _COUNTER_INVALIDATE_FORMAT_RECORD;
	
	/**
	 * {@link ComputedColumns} specified by user.
	 */
	protected List<ComputedColumns> computedColumns = null;
	
	protected boolean _IS_MAP_ONLY_JOB = false;
	
	public static final long _100MB = 100L*1024L*1024L;
	
	protected boolean reporterSet = false;
	
	private static final Log LOGGER = LogFactory.getLog(AbstractMobiusMapper.class);
	
	
	/**
	 * Setup Mapper.
	 * <p>
	 * 
	 * Override this method if there is extra initial
	 * settings need to be done.
	 * <p>
	 * 
	 * Make sure to call <code>super.configure(JobConf)</code>
	 * when overriding.
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void configure(JobConf conf)
	{
		super.configure(conf);		
		
		this.conf = conf;
		
		this._IS_MAP_ONLY_JOB = this.conf.getInt("mapred.reduce.tasks", 1)==0;
		
		// catch the current dataset ID, the {@link Configuration#get(String)}
		// is costly as it compose Pattern every time. 
		this.currentDatasetID = this.conf.get (ConfigureConstants.CURRENT_DATASET_ID);
		
		
		 // initialize counters
		this._COUNTER_INPUT_RECORD				= 0L;
		this._COUNTER_OUTPUT_RECORD				= 0L;
		this._COUNTER_FILTERED_RECORD			= 0L;
		this._COUNTER_INVALIDATE_FORMAT_RECORD 	= 0L;
		
		this.dataset_display_id = this.getDatasetID().substring(this.getDatasetID().indexOf("_")+1);
		
		try
		{
			this.key_columns	= (String[])this.conf.getStrings(this.getDatasetID()+".key.columns", Util.ZERO_SIZE_STRING_ARRAY);			
			this.value_columns	= (String[])this.conf.getStrings(this.getDatasetID()+".value.columns", Util.ZERO_SIZE_STRING_ARRAY);
			this.tuple_criteria = (TupleCriterion)this.get("tuple.criteria");
			this.computedColumns= (List<ComputedColumns>)this.get("computed.columns");
			
			if( this._IS_MAP_ONLY_JOB )
			{
				this.projection_order = (String[])this.conf.getStrings(this.getDatasetID()+".columns.in.original.order", Util.ZERO_SIZE_STRING_ARRAY);
			}
		}catch(IOException e)
		{
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * map()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void joinmap(IK key, IV value, OutputCollector<WritableComparable<?>, WritableComparable<?>> output, Reporter reporter)
		throws IOException 
	{
		// initializing counter updating thread, to be run in the background. 
		if( this.counterThread==null )
		{
			this.counterThread = new CounterUpdateThread(reporter);
			new Thread(this.counterThread).start();
		}
		
		if (!reporterSet){
			if( this.computedColumns!=null ){
				for( ComputedColumns c:this.computedColumns ){
					c.setReporter(reporter);
				}
			}
			reporterSet = true;
		}
		
		Tuple record = null;
		try
		{
			record = this.parse(key, value);
		}
		catch(IllegalFormatException e)
		{
			this._COUNTER_INVALIDATE_FORMAT_RECORD++;
			this.updateCounter(this.dataset_display_id, "INVALIDATE_RECORDS", this._COUNTER_INVALIDATE_FORMAT_RECORD);
			return;
		}
		
		this._COUNTER_INPUT_RECORD++;
		this.updateCounter(this.dataset_display_id, "INPUT_RECORDS", this._COUNTER_INPUT_RECORD);
		
		
		Iterable<Tuple> rows_to_be_output = new ArrayList<Tuple>();
		((List<Tuple>)rows_to_be_output).add(record);
		
		// apply computed column if any
		if( this.computedColumns!=null )
		{
			for( ComputedColumns aComputedColumn:this.computedColumns )
			{
				aComputedColumn.reset();
				aComputedColumn.consume(Tuple.immutable(record));
				
				if ( aComputedColumn.getResult()!=null && aComputedColumn.getResult().size()>0 )
				{
					BigTupleList computedResult = aComputedColumn.getResult();
					if( computedResult.size()<5000 )
					{
						// use in memory cross product
						Iterable<Tuple>[] allValues = new Iterable[2];
						allValues[0] = rows_to_be_output;
						allValues[1] = aComputedColumn.getResult();
						
						rows_to_be_output = Util.inMemoryCrossProduct(allValues);
					}
					else
					{
						// computed result is too big, don't use in memory cross
						// product.
						Iterable<Tuple>[] allValues = new Iterable[2];
						allValues[0] = rows_to_be_output;
						allValues[1] = aComputedColumn.getResult();
						
						rows_to_be_output = Util.crossProduct(this.conf, reporter,  allValues);
					}
				}
			}			
		}
		
		
		// apply the criteria if any and prepare output
		for( Tuple aRow:rows_to_be_output)
		{
			Tuple out_key	= this.getKeyTuple(this.key_columns, aRow, null);
			Tuple out_value = null;
			if( !this._IS_MAP_ONLY_JOB )
			{
				// tuple will go to reducer phase, we use the sorted column
				// so the reducer can set the schema back correctly.
				out_value = this.getTuple(this.value_columns, aRow, Tuple.NULL);
			}
			else
			{
				out_value = this.getTuple(this.projection_order, aRow, Tuple.NULL);
			}
			
				
			/**
			 * TODO some tuple criteria can be applied earlier, if the 
			 * columns it is evaluating are not derived.  This can 
			 * save the time to compute the derived columns, as it might
			 * be costly. 
			 */
			if( this.tuple_criteria!=null )
			{
				// use the aRow as the criteria might use column(s)
				// not within the projection columns (<code>value_columns</code>).
				if ( this.tuple_criteria.accept(aRow, this.conf) ) 
				{
					outputRecords(out_key, out_value, output);
					this._COUNTER_OUTPUT_RECORD++;
					this.updateCounter(this.dataset_display_id, "OUTPUT_RECORDS", this._COUNTER_OUTPUT_RECORD);
				}
				else
				{
					this._COUNTER_FILTERED_RECORD++;
					this.updateCounter(this.dataset_display_id, "FILTERED_RECORDS", this._COUNTER_FILTERED_RECORD);
				}
			}
			else
			{
				outputRecords(out_key, out_value, output);
				this._COUNTER_OUTPUT_RECORD++;
				this.updateCounter(this.dataset_display_id, "OUTPUT_RECORDS", this._COUNTER_OUTPUT_RECORD);
			}
		}
		
		if( rows_to_be_output instanceof Closeable )
		{
			((Closeable)rows_to_be_output).close();
		}
	}
	
	protected void outputRecords(Tuple key, Tuple value, OutputCollector<WritableComparable<?>, WritableComparable<?>> output)
		throws IOException
	{
		if( this._IS_MAP_ONLY_JOB )
		{
			// map only job, key is not needed as no join is required.
			output.collect(NullWritable.get(), value);
		}
		else
		{
			if( key==null )
			{
				// should never happen, this is to perform join/group by, but there
				// is no key
				throw new IllegalArgumentException("key for dataset: "+this.getDatasetID()+
						" cannot be empty when performing join/group by.");
			}
			output.collect(key, value);
		}
	}
	
	/**
	 * close Mapper
	 */
	@Override
	public void close()
		throws IOException
	{		
		this.counterThread.stop();
	}
	
	/**
	 * Parse the input key and input value into {@link Tuple}
	 */
	public abstract Tuple parse(IK inkey, IV invalue)
		throws IllegalArgumentException, IOException;
	
	
	/**
	 * update certain counter
	 */
	protected final void updateCounter(String group, String couter, long number)
	{
		this.counterThread.updateCounter(group, couter, number);
	}
	
	
	/**
	 * Get the current dataset ID.
	 */
	public final String getDatasetID()
	{
		return this.currentDatasetID;	
	}
	
	/**
	 * Get object from {@link JobConf}, assuming the value 
	 * is Base64 encoded, and can be decoded back to Java
	 * object.
	 * <p>
	 * 
	 * If the value from {@link JobConf} for the given 
	 * <code>key</code> is null or empty, null is returned.
	 */
	protected final Object get(String key) throws IOException
	{
		String value = this.conf.get(this.getDatasetID()+"."+key);
		if( value==null || (value=value.trim()).isEmpty() )
			return null;
		
		return SerializableUtil.deserializeFromBase64(value, this.conf);
	}
	
	private final Tuple getKeyTuple(String[] columns, Tuple record, Tuple defaultValue)
	{
		if( columns==null || columns.length==0 )
			return defaultValue;
		else
		{
			Tuple t = new KeyTuple();
			for( String aColumn:columns )
			{
				t.insert(aColumn, record.get(aColumn));
			}
			return t;
		}
	}
	
	/**
	 * retrieve columns from the record tuple, and return
	 * a new tuple instance which contains only the specified
	 * columns.
	 */
	private final Tuple getTuple(String[] columns, Tuple record, Tuple defaultValue)
	{
		if( columns==null || columns.length==0 )
			return defaultValue;
		else
		{
			Tuple t = new Tuple();
			for( String aColumn:columns )
			{
				t.insert(aColumn, record.get(aColumn));
			}
			return t;
		}
	}
	
	
}
