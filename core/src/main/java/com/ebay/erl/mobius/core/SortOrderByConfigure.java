package com.ebay.erl.mobius.core;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.mapred.JobConf;

import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.sort.Sorter;
import com.ebay.erl.mobius.util.SerializableUtil;

/**
 * Specifies the sort-by columns (ascending or 
 * descending) of a sort job.
 * <p>
 * 
 * See {@link MobiusJob#sort(Dataset)} for information on
 * creating a sort job.
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
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Jack Shen, Gyanit Singh, Neel Sundaresan
 */
@SuppressWarnings("deprecation")
public class SortOrderByConfigure 
{
	
	private JobConf conf;
	
	/**
	 * the {@link Dataset} to be sorted.
	 */
	private Dataset aDataset;
	
	private Set<Column> projections;
	
	SortOrderByConfigure(JobConf conf, Dataset dataset, Column[] projections)
	{
		this.conf		= conf;
		this.aDataset	= dataset;
		this.projections= new LinkedHashSet<Column>();
		for( Column aColumn:projections )
		{
			this.projections.add(aColumn);
		}
	}
	
	
	/**
	 * Specify the dataset to be sorted by the <code>sorters</code>
	 * <p>
	 * 
	 * The <code>sorters</code> can only access the columns within
	 * the projection list of this job.
	 */
	public SortPersistable orderBy(Sorter... sorters)
		throws IOException
	{
		if( sorters==null || sorters.length==0 )
			throw new IllegalArgumentException("Please specify at least one sorter.");
		
		// validate if the columns in the <code>sorters</code> are
		// all the selected <code>projections</code>.
		
		for( Sorter aSorter:sorters )
		{
			if( !this.projections.contains(new Column(this.aDataset, aSorter.getColumn())) )
			{
				throw new IllegalArgumentException(aSorter.getColumn()+" does not in the projection list, " +
						"please use only columns that are specified in the "+SortProjectionConfigure.class.getSimpleName()+"#select(Column) method.");
			}
		}
		
		// set the columns to be emitted as key for this sort job.  These
		// columns are the one used in the <code>Sorters</code>
		String outputKey = this.aDataset.getID()+".key.columns";		
		StringBuffer columns = new StringBuffer();
		for( int i=0;i<sorters.length;i++ )
		{
			columns.append(sorters[i].getColumn());
			if( i<sorters.length-1 )
				columns.append(",");
		}
		this.conf.set(outputKey, columns.toString());
		
		// store the sorter objects
		this.conf.set(ConfigureConstants.SORTERS, SerializableUtil.serializeToBase64(sorters));
		
		this.conf.set("mapred.job.name", "Total Sort "+this.aDataset.getName()+" by "+Arrays.toString(sorters));
		
		return new SortPersistable(this.conf, this.projections);
	}
}
