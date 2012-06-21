package com.ebay.erl.mobius.core;

/**
 * Defines Mobius specific property
 * keys to be set in different kinds of Mobius
 * jobs in the Hadoop Configuration.
 * <p>
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
public class ConfigureConstants 
{	
	
	/**
	 * Indicate the current {@link com.ebay.erl.mobius.core.builder.Dataset} ID processed by a mapper.
	 */
	public static final String CURRENT_DATASET_ID			= "mobius.current.dataset.id";
	
	
	
	/** 
	 * the delimiter to be used in {@link com.ebay.erl.mobius.core.model.Tuple#toString()}, default is tab.
	 * <p>
	 * 
	 * The toString method is called if the output format is text.
	 * <p>
	 * 
	 * To change the delimiter of Tuple, modify this parameter in command line like: 
	 * -Dmobius.tuple.tostring.delimiter=YOUR_DELIMITER.
	 */
	public static final String TUPLE_TO_STRING_DELIMITER	= "mobius.tuple.tostring.delimiter";
	
	
	
	/**
	 * To locate the Base64 encoded projection columns.
	 */
	public static final String PROJECTION_COLUMNS			= "mobius.projection.columns";	
	
	
	
	/**
	 * To locate the mapping from an input path to
	 * the corresponding dataset id.
	 */
	public static final String INPUT_TO_DATASET_MAPPING		= "mobius.input.to.dataset.id.mapping";
	
	
	
	/**
	 * To locate all the participated {@link com.ebay.erl.mobius.core.builder.Dataset} id in a Mobius job.
	 */
	public static final String ALL_DATASET_IDS				= "mobius.all.dataset.ids";
	
	
	
	/**
	 * To locate all the group by columns in a group by job.
	 */
	public static final String ALL_GROUP_KEY_COLUMNS		= "mobius.all.group.key.columns";
	
	
	
	/**
	 * To locate Base64 encoded {@link com.ebay.erl.mobius.core.sort.Sorter}
	 * in a sorting job.
	 */
	public static final String SORTERS						= "mobius.sorters";
	
	
	
	/**
	 * To indicate if this mobius job is a sort job or not.
	 */
	public static final String IS_SORT_JOB					= "mobius.sort.job";
	
	
	
	/**
	 * To locate the mapper class for a Mobius job.
	 */
	public static final String MAPPER_CLASS					= "mobius.mapper.class";

	
	
	/**
	 * To indicate Mobius join job is a outer join job or not.
	 */
	public static final String IS_OUTER_JOIN 				= "mobius.outer.join";

	
	
	/**
	 * To locate the object to be used to represents a null value.
	 */
	public static final String NULL_REPLACEMENT 			= "mobius.null.replacement";
	
	
	/**
	 * the type of the null replacement object.
	 */
	public static final String NULL_REPLACEMENT_TYPE		= "mobius.null.replacement.type";

	
	/**
	 * To locate the filter criteria used before a join, grouping
	 * job save the records to disk.
	 */
	public static final String PERSISTANT_CRITERIA 			= "mobius.persistant.criteria";
}
