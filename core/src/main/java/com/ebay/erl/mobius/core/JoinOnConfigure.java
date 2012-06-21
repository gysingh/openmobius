package com.ebay.erl.mobius.core;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.mapred.AbstractMobiusMapper;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.core.model.WriteImpl;
import com.ebay.erl.mobius.util.SerializableUtil;
import com.ebay.erl.mobius.util.Util;

/**
 * Specify the join relationship in a Mobius join job.
 * <p>
 * 
 * A join relationship defines the columns to be
 * used as the join keys of the participating
 * datasets.
 * <p>
 * 
 * Use {@link MobiusJob#innerJoin(Dataset...)},
 * {@link MobiusJob#leftOuterJoin(Dataset, Dataset, Object)} or
 * {@link MobiusJob#rightOuterJoin(Dataset, Dataset, Object)} to
 * obtain an instance of this class to create a join job.
 * 
 * 
 * 
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
public class JoinOnConfigure 
{	
	private Configuration jobConf;
	
	private Dataset[] datasets;
	
	// hide the constructor to default level
	JoinOnConfigure(Configuration jobConf, Dataset... datasets)
	{
		if( datasets==null||datasets.length<=1 )
		{
			throw new IllegalArgumentException("Join must be performed with at least two or more dataset.");
		}
		
		this.jobConf	= jobConf;
		this.datasets	= datasets;
	}
	
	
	JoinOnConfigure(Object nullReplacement, Configuration jobConf, Dataset... datasets)
		throws IOException
	{
		this(jobConf, datasets);
		
		// validate the type of nullReplacement
		byte type = Tuple.getType(nullReplacement);
		this.jobConf.setInt(ConfigureConstants.NULL_REPLACEMENT_TYPE, type);
		
		if( nullReplacement!=null )
		{			
			ByteArrayOutputStream buffer	= new ByteArrayOutputStream();
			DataOutputStream out 			= new DataOutputStream(buffer);
			WriteImpl writer				= new WriteImpl(out);
			writer.setValue(nullReplacement);
			writer.handle(type);
			out.flush();
			out.close();
			
			byte[] binary = buffer.toByteArray();
			String base64 = SerializableUtil.serializeToBase64(binary);
			
			this.jobConf.set(ConfigureConstants.NULL_REPLACEMENT, base64);			
		}
	}
	
	
	/**
	 * Specify the joining columns from the dataset.
	 * <p>
	 * 
	 * Where there are more than one {@link EQ} in the
	 * argument, they will be concatenated together
	 * with AND.
	 * <p>
	 * 
	 * Mobius only supports equal-join, ex: dataset1.column1=dataset2.column1.
	 */
	public Persistable on(EQ... eqs)
		throws IOException
	{
		if( eqs==null||eqs.length==0 )
		{
			throw new IllegalArgumentException ("Please set at least one join key");
		}		
		
		Set<Column> keyColumns = new HashSet<Column>();
		
		for( EQ anEQ:eqs )
		{
			for( Column aColumn:anEQ.columns )
			{
				this.setJoinKey (aColumn);
				keyColumns.add(aColumn);
			}
		}
		
		this.jobConf.set(ConfigureConstants.ALL_GROUP_KEY_COLUMNS, SerializableUtil.serializeToBase64(keyColumns.toArray(new Column[0])));
		StringBuffer involvedDSName = new StringBuffer();
		for( int i=0;i<this.datasets.length;i++ )
		{
			involvedDSName.append(this.datasets[i].getName());
			if( i<this.datasets.length-1 )
				involvedDSName.append(", ");
		}
		boolean isOuterJoin = this.jobConf.getBoolean(ConfigureConstants.IS_OUTER_JOIN, false);
		
		this.jobConf.set("mapred.job.name", (isOuterJoin?"Outer Join ":"Inner Join ")+involvedDSName.toString()+" On "+Arrays.toString(eqs));
		
		return new Persistable(new JobConf(this.jobConf), this.datasets);
	}
	
	/**
	 * set the $datasetID.key.columns so the {@link AbstractMobiusMapper} can
	 * emit the correct key to perform join.
	 * 
	 * @param aColumn
	 */
	private void setJoinKey(Column aColumn)
		throws IOException
	{
		// represent property name, $datasetID.key.columns
		String joinKeyPropertyName = null;
		
		for( int jobSeqNbr=0;jobSeqNbr<this.datasets.length;jobSeqNbr++ )
		{
			Dataset aDataset = this.datasets[jobSeqNbr];
			if( aColumn.getDataset().equals(aDataset) )
			{
				JobSetup.validateColumns(aDataset, aColumn);				
				Configuration aJobConf	= aDataset.createJobConf(jobSeqNbr);
				this.jobConf			= Util.merge(this.jobConf, aJobConf);				
				joinKeyPropertyName		= aDataset.getDatasetID(jobSeqNbr)+".key.columns";
				break;
			}
		}
		
		if ( joinKeyPropertyName==null )
		{
			throw new IllegalArgumentException (aColumn.getDataset() + " doesn't in the selected join dataset.");
		}
		
		if( this.jobConf.get(joinKeyPropertyName, "").isEmpty() )
		{
			this.jobConf.set (joinKeyPropertyName, aColumn.getInputColumnName ());
		}
		else
		{
			this.jobConf.set (joinKeyPropertyName, this.jobConf.get (joinKeyPropertyName) + "," + aColumn.getInputColumnName ());
		}

	}
	
	
	/**
	 * Specify the equal relationship
	 * of columns from different datasets.
	 * <p>
	 * 
	 * Each {@link EQ} is used in a join job
	 * as one of the join conditions.
	 */
	public static class EQ
	{
		private Column[] columns;
		
		/**
		 * Build a equal join condition from columns
		 * in the participated datasets in a join job.
		 * 
		 * 
		 * @param columns  Columns from different datasets
		 * to be used to build equal relationship.
		 */
		public EQ(Column... columns)
		{
			// validation, lengh of columns
			// must >=2, and all from different
			// dataset
			
			if( columns==null  || columns.length<2 )
				throw new IllegalArgumentException("Please specify at least two columns to form a equal relation.");
			
			// make sure columns all from different dataset
			Set<Dataset> ds = new HashSet<Dataset>();
			for( Column aColumn:columns )
				ds.add(aColumn.getDataset());
			
			if( ds.size()!=columns.length )
				throw new IllegalArgumentException("The specified columns must from different datasets.");
			
			this.columns = columns;
		}
		
		public String toString()
		{
			StringBuffer str = new StringBuffer();
			for( int i=0;i<this.columns.length;i++ )
			{
				str.append(this.columns[i].getInputColumnName());
				if( i<this.columns.length-1 )
					str.append(" = ");
			}
			return str.toString();
		}
	}
}
