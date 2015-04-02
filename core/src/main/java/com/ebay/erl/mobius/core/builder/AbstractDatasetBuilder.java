package com.ebay.erl.mobius.core.builder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import com.ebay.erl.mobius.core.MobiusJob;
import com.ebay.erl.mobius.core.criterion.TupleCriterion;
import com.ebay.erl.mobius.core.model.ComputedColumns;

/**
 * The base class of all {@link Dataset} builders which builds
 * instance of different {@link Dataset}.
 * <p>
 * 
 * This product is licensed under the Apache License,  Version 2.0, 
 * available at http://www.apache.org/licenses/LICENSE-2.0.
 * 
 * This product contains portions derived from Apache hadoop which is 
 * licensed under the Apache License, Version 2.0, available at 
 * http://hadoop.apache.org.
 * 
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Jack Shen, Gyanit Singh, Neel Sundaresan
 *
 * @param <ACUTAL_BUILDER_IMPL> the implementation of a {@link AbstractDatasetBuilder}.
 */
@SuppressWarnings({"unchecked", "deprecation"})
public abstract class AbstractDatasetBuilder<ACUTAL_BUILDER_IMPL>
{
	private static final Log LOGGER = LogFactory.getLog(AbstractDatasetBuilder.class);
	
	private Dataset dataset;
	
	/**
	 * An instance of {@link MobiusJob} which contains
	 * the analysis flow.
	 */
	protected MobiusJob mobiusJob;
	
	/**
	 * name of this dataset.
	 */
	protected String datasetName;
	
	
	/**
	 * The {@link ComputedColumns} for this dataset.
	 */
	protected List<ComputedColumns> computedColumns;
	
	
	/**
	 * Constructor for creating a dataset builder.
	 */
	protected AbstractDatasetBuilder(MobiusJob aJob, String datasetName)
	{
		this.mobiusJob = aJob;
		this.datasetName = datasetName;
	}
	
	
	/**
	 * Get the {@link #dataset}, if it's null,
	 * then {@link #newDataset(String)} will be called
	 * and assign {@link #dataset} to the return object.
	 */
	protected Dataset getDataset()
	{
		if( this.dataset==null )
			this.dataset = this.newDataset(this.datasetName);
		
		return this.dataset;
	}
	
	/** 
	 * Create a new {@link Dataset}, the returned {@link Dataset}
	 * has no state at all (no paths, constraints...etc.) 
	 * 
	 */
	protected abstract Dataset newDataset(String datasetName);
	
	
	/**
	 * To be called by Mobius engine, for building a dataset from
	 * a previous mobius job, user should not use this method.
	 */
	public Dataset buildFromPreviousJob(JobConf prevJob, Class<? extends FileOutputFormat> prevJobOutputFormat, String[] schema)
		throws IOException
	{
		// default implementation
		throw new UnsupportedOperationException("");
	}
	
	
	/**
	 * Specify the schema of this {@link Dataset}
	 */
	public ACUTAL_BUILDER_IMPL setSchema(String... schema )
	{
		this.getDataset().setSchema(schema);
		return (ACUTAL_BUILDER_IMPL)this;
	}
	
	
	/**
	 * Add a {@link ComputedColumns} to this dataset.
	 * 
	 * @see com.ebay.erl.mobius.core.model.ComputedColumns
	 */
	public ACUTAL_BUILDER_IMPL addComuptedColumn(ComputedColumns aComputedColumn)
	{
		this.getDataset().addComputedColumn(aComputedColumn);		
		return (ACUTAL_BUILDER_IMPL)this;
	}
	
	
	/**
	 * Finishing the {@link Dataset} building process.
	 * <p>
	 * 
	 * Invoke this method to get an reference to a {@link Dataset}
	 * so it can be used in {@link MobiusJob#innerJoin(Dataset...)},
	 * {@link MobiusJob#list(Dataset, com.ebay.erl.mobius.core.model.Column...)}
	 * ...etc.
	 * 
	 * @return an instance of {@link Dataset}
	 * @throws IllegalStateException when the user doesn't specify all the required 
	 * parameters (no input path, for example) during the building process.
	 */
	public Dataset build() throws IllegalStateException
	{
		this.getDataset().validate();
		Dataset result	= this.getDataset();
		
		// set the dataset to null to allow building a complete new dataset
		this.dataset	= null; 
		return result;
	} 
	
	
	/**
	 * Specify the input path(s) of a {@link Dataset}.
	 * 
	 * @param paths one or more path that contain the dataset of
	 * @return the builder itself.
	 * @throws IOException
	 */	
	public ACUTAL_BUILDER_IMPL addInputPath(Path... paths)
		throws IOException
	{
		for(Path anInput:paths)
		{
			LOGGER.info("Adding an input path:"+anInput.toString());			
			if( this.mobiusJob.isOutputOfAnotherJob(anInput) )
			{
				this.addInputPath(false, anInput);
			}
			else
			{
				this.addInputPath(true, anInput);
			}
		}
		return (ACUTAL_BUILDER_IMPL)this;
	}
	
	/**
	 * Check if there is a touch file exist within the given <code>aFolder</code>.
	 * <p>
	 * This method is invoked when user use {@link #addInputPath(Path...)}, and
	 * return true by default, i.e., do not check touch file.  Touch file is
	 * used in to indicate the files for a dataset are all ready, if the
	 * deployed Hadoop system will generate touch file for a Hadoop output folder,
	 * user should override this method to enable the touch file checking.  
	 * 
	 * @param fs
	 * @param aFolder
	 * @return true in default implementation.
	 */
	protected boolean checkTouchFile(FileSystem fs, Path aPath)
	{
		return true;
	}
	
	
	/**
	 * Add the <code>paths</code> to the underline dataset.  A boolean
	 * flag <code>validatePathExistance</code> to specify if Mobius
	 * needs to verify the specified <code>paths</code> exist or not.
	 * <p>
	 * 
	 * If <code>validatePathExistance</code> is true, and one of the
	 * <code>paths</code> doesn't exist, <code>IOException</code> will
	 * be thrown.
	 * <p>
	 * 
	 * If a path exists and it's a folder, {@link #checkTouchFile(FileSystem, Path)} 
	 * will be called to see if a touch file exists under that folder or not.
	 * The default implementation of <code>checkTouchFile</code> always return
	 * true, which means the dataset builder doesn't check touch file by default.
	 * If this is a need to check touch file, the subclass should override that
	 * function, and when the funciton return false, <code>IOException</code>
	 * will be thrown here for that specific path.
	 */
	protected ACUTAL_BUILDER_IMPL addInputPath(boolean validatePathExistance, Path...paths)
		throws IOException
	{
		if( paths==null || paths.length==0 )
		{
			throw new IllegalArgumentException("Please specify at least one path");
		}
		
		FileSystem fs = FileSystem.get(this.mobiusJob.getConf());
		
		for( Path aPath:paths )
		{
			FileStatus[] fileStatus = null;
			
			try
			{
				fileStatus = fs.globStatus (aPath);
			}catch(NullPointerException e)			
			{
				LOGGER.warn("FileSystem list globStatus thrown NPE", e);
			}
			
			if( fileStatus==null )
			{
				if( validatePathExistance )
				{
					throw new FileNotFoundException(aPath.toString ()+" doesn't exist on file system.");
				}
				else
				{
					// no need to validate, as the input
					// for this dataset is coming from
					// the output of the other dataset.
					this.getDataset().addInputs (aPath);
				}
			}
			else
			{
				// file(s) exists, add inputs
				for( FileStatus aFileStatus:fileStatus )
				{
					Path p = aFileStatus.getPath();
					if( !fs.isFile(p) )
					{
						if( !this.checkTouchFile(fs, p) )
						{
							throw new IllegalStateException("No touch file under "+p.toString()+", this dataset is not ready.");
						}
						else
						{
							this.getDataset().addInputs (p);
						}
					}
					else
					{
						this.getDataset().addInputs (p);
					}
				}
			}
		}
		
		return (ACUTAL_BUILDER_IMPL)this;
	}
	
	/**
	 * Put filter on the records of this {@link Dataset}, only
	 * raw within the {@link Dataset} that meet the criteria
	 * can be outputed.
	 * <p>
	 *
	 * @param criteria
	 * @return the builder itself.
	 */
	public ACUTAL_BUILDER_IMPL constraint(TupleCriterion criteria)
	{
		Dataset dataset = this.getDataset();		
		TupleCriterion.validate(dataset.getSchema(), criteria);
		this.getDataset().addConstraint(criteria);
		return (ACUTAL_BUILDER_IMPL)this;
	}
}
