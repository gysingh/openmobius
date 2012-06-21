package com.ebay.erl.mobius.core.builder;

import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.ebay.erl.mobius.core.MobiusJob;
import com.ebay.erl.mobius.core.criterion.LogicalExpression;
import com.ebay.erl.mobius.core.criterion.TupleCriterion;
import com.ebay.erl.mobius.core.criterion.LogicalExpression.Operator;
import com.ebay.erl.mobius.core.mapred.AbstractMobiusMapper;
import com.ebay.erl.mobius.core.model.ComputedColumns;
import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.core.model.Tuple.TupleColumnName;
import com.ebay.erl.mobius.core.sort.Sorter;
import com.ebay.erl.mobius.util.SerializableUtil;

/**
 * Represents a type of data on the Hadoop cluster.
 * <p>
 * 
 * A dataset contains the following information:
 * <ul>
 * 	<li>{@link org.apache.hadoop.mapred.InputFormat} specifies the format for the
 * 		Hadoop to use to read the data.</li>
 * 	<li>The schema provides Mobius information on all the available column names 
 * 		in this dataset.</li>
 * 	<li>An array of {@link org.apache.hadoop.fs.Path} indicates the 
 * 		data location of this dataset.</li>
 * 	<li>A sub-class implementation of {@link AbstractMobiusMapper} provides 
 * 		Hadoop the information on mapper class.</li>
 * </ul>
 * <p>
 * 
 * An instance of {@link Dataset} is built by an implementation of 
 * {@link AbstractDatasetBuilder}.
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
@SuppressWarnings({ "unchecked", "deprecation" })
public class Dataset implements Serializable
{	
	private static final DecimalFormat _TWO_DIGITS = new DecimalFormat("00");
	
	private static final long serialVersionUID = 9154263294942025327L;
	
	private final int hashCode;
	
	/**
	 * a list of {@link Path} contain the data of this dataset 
	 */
	private transient List<Path> inputs;
	
	
	/**
	 * The {@link InputFormat} of this dataset, so Hadoop knows how to 
	 * read this mapper.
	 */	
	protected Class<? extends InputFormat> input_format;
	
	
	/**
	 * The corresponding {@link AbstractMobiusMapper} implementation which
	 * parse the records of this dataset input {@link Tuple}.
	 */
	protected Class<? extends AbstractMobiusMapper> mapper;
	
	
	/**
	 * the tuple constraint.
	 * <p>
	 * 
	 * If specified, only tuples that pass
	 * this constraint will be emitted.
	 */
	protected TupleCriterion tupleConstraint;
	
	
	/**
	 * Hadoop configuration.
	 */
	protected transient Configuration conf;
	
	
	/**
	 * To store user defined {@link ComputedColumns}
	 * of this dataset, if any.
	 */
	protected transient ArrayList<ComputedColumns> computedColumns;  
	
	
	/**
	 * The schema of this {@link Dataset}, using
	 * {@link LinkedHashSet} to preserve the 
	 * schema order.
	 */
	protected LinkedHashSet<String> schema;
	
	
	/**
	 * name of this dataset.
	 */
	protected String name;
	
	
	/**
	 * The mobius job contains the ananlysis flow.
	 */
	protected transient MobiusJob job;
	
	
	
	
	protected Dataset(MobiusJob job, String name)
	{
		this.initialize();
		this.conf = job.getConf();
		this.name = name;
		this.job = job;
		
		this.hashCode = this.name.hashCode();
	}
	
	/**
	 * Get the schema of this {@link Dataset}.
	 * <p>
	 * 
	 * The returned set is a <code>LinkedHashSet</code>,
	 * schema is sorted in the insertion order.
	 *
	 */
	protected LinkedHashSet<String> getSchema()
	{
		return this.schema;
	}
	
	
	/**
	 * Create a Hadoop JobConf that represents this dataset.
	 * <p>
	 * 
	 * This method is called by Mobius.
	 */
	public JobConf createJobConf(int jobSequenceNumber)
		throws IOException
	{
		// preparing to create the new job, write the job conf
		
		if ( this.tupleConstraint!=null)
			this.conf.set(this.getDatasetID(jobSequenceNumber)+".tuple.criteria", SerializableUtil.serializeToBase64(this.tupleConstraint));
		
		StringBuffer schemaStr = new StringBuffer();
		Iterator<String> it = this.getSchema().iterator();
		while( it.hasNext() )
		{
			schemaStr.append(it.next());
			if( it.hasNext() )
				schemaStr.append(",");
		}
		this.conf.set(this.getDatasetID(jobSequenceNumber)+".schema", schemaStr.toString());
		
		// setup computed columns, if any
		if( this.computedColumns!=null && this.computedColumns.size()>0 )
		{
			this.conf.set(this.getDatasetID(jobSequenceNumber)+".computed.columns", SerializableUtil.serializeToBase64(this.computedColumns));
		}
		
		return new JobConf(this.conf);
	}
	
	
	/**
	 * Get the ID for this dataset.
	 * <p>
	 * 
	 * A dataset id is composed of two digits
	 * of integer (from the <code>jobSequenceNumber</code>)
	 * and the name of the dataset.
	 * <p>
	 * 
	 * This method is used by Mobius engine only.
	 */
	public String getDatasetID(int jobSequenceNumber)
	{
		return _TWO_DIGITS.format(jobSequenceNumber)+"_"+this.name;
	}
	
	
	/**
	 * Specified the schema of this dataset.
	 */
	protected void setSchema(String... schema)
	{
		if( this.schema==null )
			this.schema = new LinkedHashSet<String>();
		
		this.schema.clear();
		
		for( String aSchema:schema )
		{
			this.schema.add(aSchema.toLowerCase());
		}
	}
	
	
	/**
	 * Add a {@link ComputedColumns} to this dataset.
	 * <p>
	 * 
	 * This method is called by an implementation of 
	 * {@link AbstractDatasetBuilder}.
	 */
	protected void addComputedColumn(ComputedColumns aComputedColumn)
	{
		if( this.computedColumns==null )
		{
			this.computedColumns = new ArrayList<ComputedColumns>();
		}
		this.computedColumns.add(aComputedColumn);
		
		// add the computed column name into the data schema
		for(String aOutputColumn:aComputedColumn.getOutputSchema() )
		{
			if( this.withinSchema(aOutputColumn) )
			{
				// there is already a column with same name exist,
				// throw exception
				throw new IllegalArgumentException(aOutputColumn+" has been defined in this dataset:"+this.toString()+", please choose another name.");
			}
			this.schema.add(aOutputColumn.toLowerCase());
		}
	}	
	
	/**
	 * The initializer, this is called everytime when a
	 * new {@link Dataset} instance is created by a
	 * {@link AbstractDatasetBuilder}
	 */
	protected void initialize()
	{
		this.inputs = new LinkedList<Path>();
		this.tupleConstraint = null;
		this.input_format = null;
		this.mapper = null;
	}
	
	/**
	 * this method is set the default level, so that
	 * only the {@link AbstractDatasetBuilder#addInputPath(Path...)}
	 * can call this method.
	 */
	void addInputs(Path...paths)
	{
		for( Path aPath:paths )
			this.inputs.add(aPath);
	}
	
	void addConstraint(TupleCriterion aConstraint)
	{
		if( this.tupleConstraint==null )
		{
			this.tupleConstraint = aConstraint;
		}
		else
		{
			this.tupleConstraint = new LogicalExpression(this.tupleConstraint, aConstraint, Operator.AND);
		}
	}
	
	
	/**
	 * Specified the {@link InputFormat} of this dataset.
	 * 
	 * This method is called by the corresponding implementation 
	 * of {@link AbstractDatasetBuilder}.
	 */
	protected void setInputFormat(Class<? extends InputFormat> input_format)
	{
		this.input_format = input_format;
	}
	
	
	/**
	 * Set the {@link AbstractMobiusMapper} for this dataset.
	 * <p>
	 * 
	 * This method is called by the corresponding implementation 
	 * of {@link AbstractDatasetBuilder}.
	 */
	protected void setMapper(Class<? extends AbstractMobiusMapper> mapper)
	{
		this.mapper = mapper;
	}
	
	
	/**
	 * Get the input paths of this dataset.
	 * <p>
	 * 
	 * Paths are specified by the user during the dataset
	 * building process.
	 */
	public List<Path> getInputs()
	{
		return Collections.unmodifiableList(this.inputs);
	}
	
	/**
	 * Get the {@link AbstractMobiusMapper} of this dataset.
	 */
	public Class<? extends AbstractMobiusMapper> getMapper()
	{
		return this.mapper;
	}
	
	
	/**
	 * Get the {@link InputFormat} of this dataset.
	 */
	public Class<? extends InputFormat> getInputFormat()
	{
		return this.input_format;
	}
	
	
	/**
	 * Check for a given <code>aColumn</code>, if it is defined in this dataset or not.
	 * 
	 * @param aColumn the name fo a column.
	 * @return true if the <code>aColumn</code> is defined in this dataset 
	 * (case insensitive), false other wise.
	 */
	public boolean withinSchema(String aColumn)
	{
		return this.getSchema().contains(TupleColumnName.valueOf(aColumn).getID().toLowerCase());
	}
	
	/**
	 * validate if this dataset has all the required parameter
	 */
	protected void validate()
	{
		if( this.mapper==null )
			throw new IllegalStateException("Please specify the Mapper of this dataset.");
		if( this.input_format==null )
			throw new IllegalStateException("Please specify the InputFormat of this dataset.");
		if( this.inputs==null || this.inputs.size()==0 )
			throw new IllegalStateException("Please specify the input path(s) of this dataset");
		if ( this.getSchema()==null || this.getSchema().size()==0 )
			throw new IllegalStateException("Please specify the schema of this dataset first");
	}
	
	
	/**
	 * Get the name of this dataset.
	 * <p>
	 * 
	 * The name of a dataset is specified 
	 * during the dataset building process.
	 */
	public String getName()
	{
		return this.name;
	}
	
	
	/**
	 * return a string contain the name of this dataset
	 * and its schema.
	 */
	@Override
	public String toString()
	{
		return this.name+this.getSchema().toString();
	}
	
	
	/**
	 * Return true only if the <code>obj</code>
	 * is an instance of {@linkplain Dataset},
	 * the name, input format, mapper, and the 
	 * schema of <code>this</code> and the 
	 * <code>obj</code> are both equals.
	 * Otherwise, false.
	 */
	@Override
	public boolean equals(Object obj)
	{
		if( obj instanceof Dataset)
		{
			if( this==obj )
				return true;
			
			Dataset that = (Dataset)obj;
			
			boolean equals = this.name.equals(that.name) && 
				this.input_format.equals(that.input_format) &&
				this.mapper.equals(that.mapper) &&
				this.schema.equals(that.schema);
			
			return equals;
		}
		
		return false;
	}
	
	
	
//	/**
//	 * Right outer join <code>this</code> dataset with <code>another</code>
//	 * dataset.
//	 * <p>
//	 * 
//	 * <code>this</code> dataset is the right one, and the <code>another</code>
//	 * dataset is the left one.  All the rows from <code>another</code> dataset
//	 * set will be emitted, when no match case happened, the values from 
//	 * <code>this</code> dataset will be replaced by the specified 
//	 * <code>nullReplacement</code>.
//	 * 
//	 * @see MobiusJob#rightOuterJoin(Dataset, Dataset, Object)
//	 */
//	public JoinOnConfigure rightOuterJoin(Dataset another, Object nullReplacement)
//		throws IOException
//	{
//		if( another==null)
//		{
//			throw new IllegalArgumentException("dataset cannot be null.");
//		}
//		
//		return this.job.rightOuterJoin(this, another, nullReplacement);
//	}
	
	
	
	
//	/**
//	 * Left outer join <code>this</code> dataset with <code>another</code>
//	 * dataset.
//	 * <p>
//	 * 
//	 * <code>this</code> dataset is the left one, and the <code>another</code>
//	 * dataset is the right one.  All the rows from <code>this</code> dataset
//	 * set will be emitted, when no match case happened, the values from 
//	 * <code>another</code> dataset will be replaced by the specified 
//	 * <code>nullReplacement</code>.
//	 * 
//	 * @see MobiusJob#leftOuterJoin(Dataset, Dataset, Object)
//	 */
//	public JoinOnConfigure leftOuterJoin(Dataset another, Object nullReplacement)
//		throws IOException
//	{
//		if( another==null)
//		{
//			throw new IllegalArgumentException("dataset cannot be null.");
//		}
//		
//		return this.job.leftOuterJoin(this, another, nullReplacement);
//	}
	
	
	
//	/**
//	 * Inner join this dataset with <code>others</code>.  The 
//	 * number of <code>others</code> dataset must greater or
//	 * equals to one.
//	 */
//	public JoinOnConfigure innerJoinWithOthers(Dataset...others)
//	{
//		if( others==null|| others.length==0 )
//		{
//			throw new IllegalArgumentException("The inner join argument must has at least one dataset.");
//		}
//		
//		Dataset[] allDataset	= new Dataset[others.length+1];
//		allDataset[0]			= this;
//		
//		for( int i=0;i<others.length;i++)
//		{
//			allDataset[i+1] = others[i];
//		}
//		
//		return this.job.innerJoin(allDataset);
//	}
	
	
	
	/**
	 * Sort this {@linkplain Dataset} by the given <code>sorters</code>.
	 * <p>
	 * 
	 * <ul>
	 * <li>Output columns: all the columns in this dataset in its original order.</li>
	 * <li>Output format: {@link SequenceFileOutputFormat} (binary output)</li>
	 * <li>Output path: a temp file under hadoop.tmp.dir</li>
	 * </ul>
	 */
	public Dataset orderBy(Sorter... sorters)
		throws IOException
	{
		return this.orderBy(SequenceFileOutputFormat.class, sorters);
	}
	
	
	
	/**
	 * Sort this {@linkplain Dataset} by the given <code>sorters</code>.
	 * <p>
	 * 
	 * <ul>
	 * <li>Output columns: all the columns in this dataset in its original order.</li>
	 * <li>Output format: user specified <code>outputformat</code></li>
	 * <li>Output path: a temp file under hadoop.tmp.dir</li>
	 * </ul>
	 */
	public Dataset orderBy(Class<? extends FileOutputFormat> outputformat, Sorter... sorters)
		throws IOException
	{
		return this.orderBy(this.job.newTempPath(), outputformat, sorters);
	}
	
	
	
	/**
	 * Sort this {@linkplain Dataset} by the given <code>sorters</code>.
	 * <p>
	 * 
	 * <ul>
	 * <li>Output columns: all the columns in this dataset in its original order.</li>
	 * <li>Output format: {@link TextOutputFormat} (text output)</li>
	 * <li>Output path: user specified <code>output</code></li>
	 * </ul>
	 */
	public Dataset orderBy(Path output, Sorter... sorters)
		throws IOException
	{
		return this.orderBy(output, TextOutputFormat.class, sorters);
	}
	
	
	
	/**
	 * Sort this {@linkplain Dataset} by the given <code>sorters</code>.
	 * <p>
	 * 
	 * <ul>
	 * <li>Output columns: all the columns in this dataset in its original order.</li>
	 * <li>Output format: user specified <code>outputformat</code></li>
	 * <li>Output path: user specified <code>output</code></li>
	 * </ul>
	 */
	public Dataset orderBy(Path output, Class<? extends FileOutputFormat> outputformat, Sorter... sorters)
		throws IOException
	{	
		return 
		this
			.job
			.sort(this)
			.select(this.getSchema().toArray(new String[0]))
			.orderBy(sorters)
			.save(job, output, outputformat);
	}
	
	
	@Override
	public int hashCode()
	{
		return this.hashCode;
	}
}
