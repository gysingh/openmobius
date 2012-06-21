package com.ebay.erl.mobius.core.builder;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import com.ebay.erl.mobius.core.MobiusJob;
import com.ebay.erl.mobius.core.mapred.AbstractMobiusMapper;
import com.ebay.erl.mobius.core.mapred.SequenceFileMapper;

/**
 * Represents a dataset backed with Hadoop sequence file
 * format.
 * 
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
 * 
 * @see SeqFileDatasetBuilder
 */
@SuppressWarnings({"deprecation", "unchecked"})
public class SeqFileDataset extends Dataset
{
	private static final long serialVersionUID = 4598460467664686790L;
	
	/**
	 * Creating an instance of {@link SeqFileDataset} with 
	 * {@link SequenceFileInputFormat} as its input format.
	 * <p>
	 * 
	 * User should not create this dataset directly through
	 * this constructor, but to use {@link SeqFileDatasetBuilder}
	 * to build one.
	 * 
	 * @param job a Mobius job contains an analysis flow.
	 * @param name name of this dataset
	 * @param mapperClass the mapper class to parse the underline values into {@linkplain Tuple}s.
	 */
	protected SeqFileDataset(MobiusJob job, String name, Class<? extends SequenceFileMapper> mapperClass) 
	{
		super(job, name);
		this.input_format	= SequenceFileInputFormat.class;
		this.mapper			= mapperClass;
	}
	

	/**
	 * Always thrown {@link UnsupportedOperationException}, the 
	 * input format for this dataset is fixed to 
	 * {@link SequenceFileInputFormat}.
	 */
	@Override
	protected void setInputFormat(Class<? extends InputFormat> input_format)
	{
		throw new UnsupportedOperationException("Cannot set different inputformat to "+SeqFileDataset.class.getSimpleName());
	}		
	
	
	/**
	 * {@inheritDoc}
	 * <p>
	 * 
	 * <code>mapper</code> must inherent {@link SequenceFileMapper}.
	 */
	@Override
	protected void setMapper(Class<? extends AbstractMobiusMapper> mapper)
	{
		if( SequenceFileMapper.class.isAssignableFrom(mapper) )
		{
			throw new UnsupportedOperationException("the mapper class must be extend from "+SequenceFileMapper.class.getSimpleName());
		}
		this.mapper = mapper;
	}
	
}
