package com.ebay.erl.mobius.core.function.base;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;

import com.ebay.erl.mobius.core.ConfigureConstants;
import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.util.SerializableUtil;

/**
 * Base class for all projection operations.
 * <p>
 *  
 * Users do not extend this class directly, and instead  extend 
 * {@link ExtendFunction}, {@link GroupFunction} or their sub-classes.
 * <p>
 * 
 * A projectable takes one to many columns from one to many datasets 
 * as it inputs, then performs calculation to generate X number 
 * of rows as the output, where X can be zero to many.  The schema of 
 * each outputted row is defined in the {@link #setOutputSchema(String...)}
 * method.  
 * <p>
 * 
 * When providing a customized implementation of a projection operation, 
 * users must make sure the output schema is consistent with the actual 
 * output.
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
 * @see GroupFunction
 * @see ExtendFunction
 */
public class Projectable implements Serializable, Configurable
{	
	private static final long serialVersionUID = 4442027462477435820L;

	
	
	/**
	 * the input columns that are required by this function to
	 * compute its result.
	 */
	protected Column[] inputs;
	
	
	
	/**
	 *  the name of the output columns in the #getResult() tuple
	 */
	protected String[] outputSchema;
	
	
	
	protected transient Configuration conf;
	
	
	protected int hashCode;
	
	
	/**
	 * a boolean flag to indicate if this function
	 * just need the key(s) in a join/group-by job
	 * only.
	 */
	private boolean useGroupKeyOnly;
	
	
	protected boolean requireDataFromMultiDatasets;
	
	protected Reporter reporter;
	
	private boolean calledByCombiner = false;
		
	
	/**
	 * Create a {@link Projectable} which takes the <code>inputs</code>
	 * to compute some result.  The schema of the result will be, by
	 * default, <code>this.getClass().getSimpleName()+"_"+aColumn.getOutputName()</code>,
	 * for each <code>inputs</code>.
	 * <p>
	 * 
	 * The number of output column doesn't have to be the same as the number of input
	 * column, user can use {@link #setOutputSchema(String...)} to set the real output
	 * schema.
	 */
	public Projectable(Column[] inputs)
	{
		this.init(inputs);
	}
	
	/**
	 * should be invoked by {@link Column} only
	 */
	protected Projectable(){}
	
	
	protected void init(Column[] inputs)
	{
		if( inputs==null || inputs.length==0 )
		{
			throw new IllegalArgumentException("Input column cannot be null nor empty");
		}
		
		this.inputs = inputs;
		this.setOutputSchema(Column.toSchemaArray(this.inputs));
		
		this.outputSchema = new String[inputs.length];
		for( int i=0;i<this.inputs.length;i++ )
		{
			this.outputSchema[i] = this.getClass().getSimpleName()+"_"+inputs[i].getOutputName();
		}
		
		StringBuffer hashBase = new StringBuffer();
		
		for( Column aColumn:this.inputs )
		{
			hashBase.append(aColumn.getInputColumnName());
		}
		this.hashCode = hashBase.toString().hashCode();
		
		this.requireDataFromMultiDatasets = this.getParticipatedDataset().size()>1;
	}
	
	
	/**
	 * true if this function require columns from 
	 * more than one dataset to compute its value.
	 */
	public final boolean requireDataFromMultiDatasets()
	{
		return this.requireDataFromMultiDatasets;
	}
	
	
	@Override
	public int hashCode()
	{
		return this.hashCode;
	}
	
	@Override
	public String toString()
	{
		StringBuffer str = new StringBuffer();
		str.append(this.getClass().getSimpleName());
		str.append("{ input(s):");
		for(Column anInput:this.getInputColumns())
		{
			str.append(anInput.getInputColumnName()).append(", ");
		}
		str.append("output(s):");
		
		String[] outputs = this.getOutputSchema();
		for( int i=0;i<outputs.length;i++ )
		{
			str.append(outputs[i]);
			if( i<outputs.length-1 )
				str.append(",");
		}
		str.append(" }");
		return str.toString();
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if( obj instanceof Projectable )
		{
			if( this==obj )
			{
				return true;
			}
			
			// equals if all the input columns are the same
			Projectable that = (Projectable)obj;
			
			if( this.getClass().equals(that.getClass()) )
			{			
				if( this.inputs.length==that.inputs.length )
				{
					for(int i=0;i<this.inputs.length;i++)
					{
						Column c1 = this.inputs[i];
						Column c2 = that.inputs[i];
						if ( !c1.equals(c2) )
						{
							return false;
						}
					}
					// all columns have been compared, return true
					return true;
				}
			}
		}
		return false;
		
	}
	
	
	
	/**
	 * Set the output schema of the result of this function.
	 */
	public Projectable setOutputSchema(String... schema)
	{
		this.outputSchema = schema;
		return this;
	}
	
	
	
	/**
	 * Get the output schema of the result of this function.
	 */
	public String[] getOutputSchema()
	{
		return this.outputSchema;
	}
	
	
	
	/**
	 * Get the input columns.
	 */
	public Column[] getInputColumns()
	{
		return this.inputs;
	}
	
	
	
	private transient Set<Dataset> uniqueInputDataset;
	
	/**
	 * return the {@link Dataset} that is required in computing
	 * the result of this function.
	 * <p>
	 * 
	 * If only one {@link Dataset} is required, this function
	 * should be applied before the cross product phase.
	 */
	public Set<Dataset> getParticipatedDataset()
	{
		if( this.uniqueInputDataset==null )
		{
			this.uniqueInputDataset = new HashSet<Dataset>();
			for(Column anInputColumn:this.inputs)
			{
				this.uniqueInputDataset.add(anInputColumn.getDataset());
			}
		}
		return Collections.unmodifiableSet(this.uniqueInputDataset);
	}



	@Override
	public Configuration getConf() 
	{
		return this.conf;
	}

	
	public final boolean useGroupKeyOnly()
	{
		return this.useGroupKeyOnly;
	}


	@Override
	public void setConf(Configuration conf) 
	{		
		this.conf = conf;
		
		this.useGroupKeyOnly = false;
		String allGroupKeyColumnsBase64 = this.conf.get(ConfigureConstants.ALL_GROUP_KEY_COLUMNS, "");
		if( !allGroupKeyColumnsBase64.isEmpty() )
		{
			try 
			{
				Column[] allKeycolumns = (Column[])SerializableUtil.deserializeFromBase64(allGroupKeyColumnsBase64, null);
				Set<Column> set = new HashSet<Column>();
				
				for(Column aColumn:allKeycolumns)
					set.add(aColumn);
				
				for( Column anInput:this.inputs )
				{
					if( !set.contains(anInput) )
					{
						this.useGroupKeyOnly = false;
						return;
					}
				}
				
				this.useGroupKeyOnly = true;
				return;
			} 
			catch (IOException e) 
			{
				throw new RuntimeException("Canont deserialize "+ConfigureConstants.ALL_GROUP_KEY_COLUMNS+" from ["+allGroupKeyColumnsBase64+"]", e);
			}
		}		
	}
	
	public void setReporter(Reporter reporter){
		this.reporter = reporter;
	}
	
	/**
	 * Determine this function can be run in a combiner or not,
	 * default is false.
	 */
	public boolean isCombinable()
	{
		if( this.inputs.length!=this.outputSchema.length )
			return false;
		
		if( this.inputs.length!=1 )
			return false;
		
		if( this.requireDataFromMultiDatasets() )
			return false;
		
		if( this.useGroupKeyOnly() )
			return true;
		
		return false;
	}
	
	
	public boolean calledByCombiner()
	{
		return this.calledByCombiner;
	}
	
	public void setCalledByCombiner(boolean calledByCombiner)
	{
		this.calledByCombiner = calledByCombiner;
	}
}
