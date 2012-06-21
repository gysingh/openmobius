package com.ebay.erl.mobius.core;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.util.Tool;

import com.ebay.erl.mobius.core.builder.AbstractDatasetBuilder;
import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.builder.DatasetBuildersFactory;
import com.ebay.erl.mobius.core.mapred.ConfigurableJob;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.core.sort.Sorter;

/**
 * Main class of the Mobius API. Extends this class 
 * to create a Mobius data processing flow.
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
@SuppressWarnings({"deprecation", "unchecked"})
public abstract class MobiusJob extends Configured implements Tool, Serializable 
{	
	private static final long serialVersionUID = -9070202196576655916L;
	
	private static final Log LOGGER = LogFactory.getLog(MobiusJob.class);
	
	transient Map<URI/*output*/, Job> jobTopology = new HashMap<URI, Job> ();
	
	transient Set<String> inputPaths = new HashSet<String>();
	
	transient List<Path> tempFiles = new LinkedList<Path>();
	
	private transient FileSystem fs;
	
	
	/**
	 * Return the Hadoop job configuration.
	 * <p>
	 * Note that, this method creates a new {@link Configuration}
	 * from the default one every time, so changes that are made 
	 * to the returned {@link Configuration} won't affect the conf 
	 * returned by the next call of {@link #getConf()}.
	 */
	@Override
	public Configuration getConf()
	{
		Configuration conf = super.getConf ()==null?new Configuration():super.getConf ();
		Configuration clone = new Configuration();
		Iterator<Entry<String, String>> it = conf.iterator ();		 
		while( it.hasNext () )
		{
			Entry<String, String> entry = it.next ();
			clone.set (entry.getKey (), entry.getValue ());
		}
		return clone;
	}
	
	/**
	 * Test if the given <code>input</code> is the output of another job or not
	 * 
	 * @param input input path of a job.
	 * @return <code>true</code> if the <code>input</code> is the output
	 * path of another job, <code>false</code> otherwise.
	 */
	public boolean isOutputOfAnotherJob(Path input)
	{
		// normalize the input first, in case of it doesn't 
		// contain schema (hdfs://, or file:// for example.)
		Path p = this.getFS().makeQualified(input);
		LOGGER.info("Current Path Key:"+this.jobTopology.keySet());
		LOGGER.info(p.toUri()+" is the output of another job? "+this.jobTopology.containsKey(p.toUri ()));
			
		return this.jobTopology.containsKey(p.toUri ());		
	}
	
	/**
	 * Test if the given <code>input</code> is the output of another job or not
	 * 
	 * @param input input path of a job
	 * @return <code>true</code> if the <code>input</code> is the output
	 * path of another job, <code>false</code> otherwise.
	 */
	public boolean isOutputOfAnotherJob(String input)
	{
		return this.isOutputOfAnotherJob(new Path(input));
	}
	
	
	
	/**
	 * Select the <code>columns</code> from the <code>dataset</code>, store
	 * it into <code>outputFolder</code> with the given <code>outputFormat</code>
	 * <p>
	 * 
	 * Here is an example:
	 * <pre>
	 * <code>
	 * public MyJob extends MobiusJob
	 * {
	 * 	public void run(String[] args)
	 * 	{
	 * 		Dataset students = ...;
	 * 		
	 * 		// save the result to $OUTPUT in SequenceFileOutputFormat,
	 * 		// the key will be NullWritable, and the value is a Tuple 
	 * 		// which contains 3 columns, id, f_name and l_name.
	 * 		this.list(students,
	 * 			new Path("$OUTPUT"),
	 * 			SequenceFileOutputFormat.class,
	 * 			new Column(students, "id"),
	 * 			new Column(students, "f_name"),
	 * 			new Column(students, "l_name")
	 * 		); 
	 * 	}
	 * 	
	 * 	public static void main(String[] args) throw Exception
	 * 	{
	 * 		System.exit(MobiusJobRunner.run(new MyJob(), args));
	 * 	}
	 * }
	 * </code>
	 * </pre>
	 */
	public Dataset list(Dataset dataset, Path outputFolder, Class<? extends FileOutputFormat> outputFormat, Column... columns)
		throws IOException
	{	
		JobConf job = dataset.createJobConf(0);
		
		job.set("mapred.job.name", dataset.getDatasetID(0));
		job.setJarByClass(this.getClass());
		job.setNumReduceTasks(0); // list is map only job
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass (Tuple.class);
		job.setJobName("List "+dataset.getName());
		
		JobSetup.validateColumns(dataset, columns);
		JobSetup.setupInputs(job, dataset, 0);
		JobSetup.setupProjections(job, dataset, 0, columns);		
		JobSetup.setupOutputs(job, outputFolder, outputFormat);
		
		this.addToExecQueue(job);
		
		AbstractDatasetBuilder builder = DatasetBuildersFactory.getInstance(this).getBuilder(outputFormat, "Dataset_"+outputFolder.getName());
		return builder.buildFromPreviousJob(job, outputFormat, Column.toSchemaArray(columns));
	}
	
	
	
	/**
	 * Select the <code>columns</code> from the <code>dataset</code> and store
	 * it into <code>outputFolder</code>.
	 * <p>
	 * The output format is {@link TextOutputFormat}.
	 * <p>
	 * 
	 * Here is an example:
	 * <pre>
	 * <code>
	 * public MyJob extends MobiusJob
	 * {
	 * 	public void run(String[] args)
	 * 	{
	 * 		Dataset students = ...;
	 * 		
	 * 		// save the result to $OUTPUT in TextOutputFormat,
	 * 		// output will be tab delimited files with 3 columns,
	 * 		// id, f_name and l_name.
	 * 		//
	 * 		// To change the delimiter, put -Dmobius.tuple.tostring.delimiter=YOUR_DELIMITER
	 * 		// when submitting a job in command line. 
	 * 		this.list(students,
	 * 			new Path("$OUTPUT"), 			
	 * 			new Column(students, "id"),
	 * 			new Column(students, "f_name"),
	 * 			new Column(students, "l_name")
	 * 		); 
	 * 	}
	 * 	
	 * 	public static void main(String[] args) throw Exception
	 * 	{
	 * 		System.exit(MobiusJobRunner.run(new MyJob(), args));
	 * 	}
	 * }
	 * </code>
	 * </pre>
	 * 
	 */
	public Dataset list(Dataset dataset, Path outputFolder, Column... columns)
		throws IOException
	{	
		return this.list(dataset, outputFolder, TextOutputFormat.class, columns);
	}
	
	
	
	/**
	 * Select the <code>columns</code> from the <code>dataset</code>.
	 * <p>
	 * 
	 * The output path is a temporal path under hadoop.tmp.dir, and the output
	 * format is {@link SequenceFileOutputFormat}.
	 * <p>
	 * 
	 * Here is an example:
	 * <pre>
	 * <code>
	 * public MyJob extends MobiusJob
	 * {
	 * 	public void run(String[] args)
	 * 	{
	 * 		Dataset students = ...;
	 * 		
	 * 		this.list(students, 
	 * 			new Column(students, "id"),
	 * 			new Column(students, "f_name"),
	 * 			new Column(students, "l_name")
	 * 		); 
	 * 	}
	 * 	
	 * 	public static void main(String[] args) throw Exception
	 * 	{
	 * 		System.exit(MobiusJobRunner.run(new MyJob(), args));
	 * 	}
	 * }
	 * </code>
	 * </pre>
	 */
	public Dataset list(Dataset dataset, Column... columns)
		throws IOException
	{	
		return this.list(dataset, this.newTempPath(), SequenceFileOutputFormat.class, columns);
	}
	
	
	/**
	 * Performing "Left Outer Join", the result contains all the records of
	 * the left {@linkplain Dataset} (the 1st {@linkplain Dataset}) with
	 * or without match to the right {@linkplain Dataset}.
	 * <p>
	 * 
	 * If in a join group, there is no records from the right {@linkplain Dataset} 
	 * (the 2nd argument), by default, <code>null</code>(if the output format is 
	 * SequenceFileOutputFormat) or empty string (if the output format is 
	 * {@link TextOutputFormat}) is written for the selected columns from 
	 * the right {@linkplain Dataset}.
	 * <p>
	 * 
	 * If <code>nullReplacement</code> is not null, then it will be used as 
	 * the value for the columns from the right dataset when no match in a 
	 * join group.
	 * <p>
	 * 
	 * To compose a <code>leftOuterJoin</code> is almost the same as composing
	 * a {@link MobiusJob#innerJoin(Dataset...)} job except that instead of calling
	 * <code>innerJoin</code>, simply change it to 
	 * <code>leftOuterJoin(Dataset, Dataset, Object)</code>. 
	 * <p>
	 * 
	 * @param left left-hand side {@link Dataset}
	 * @param right right-hand side {@link Dataset} 
	 * @param nullReplacement the value to be used as the value for null columns, 
	 * it can be only the type supported by {@link Tuple}
	 * 
	 */
	public JoinOnConfigure leftOuterJoin(Dataset left, Dataset right, Object nullReplacement)
		throws IOException
	{
		Configuration conf = this.getConf();
		conf.setBoolean(ConfigureConstants.IS_OUTER_JOIN, true);
		
		return new JoinOnConfigure(nullReplacement, conf, left, right);
	}
	
	
	
	/**
	 * Performing "Left Outer Join", the result contains all the records of
	 * the left {@linkplain Dataset} (the 1st {@linkplain Dataset}) with
	 * or without match to the right {@linkplain Dataset}.
	 * <p>
	 * 
	 * If in a join group, there is no records from the right {@linkplain Dataset} 
	 * (the 2nd argument), by default, <code>null</code>(if the output format is 
	 * SequenceFileOutputFormat) or empty string (if the output format is 
	 * {@link TextOutputFormat}) is written for the selected columns from 
	 * the right {@linkplain Dataset}.
	 * <p>
	 * 
	 * To compose a <code>leftOuterJoin</code> is almost the same as composing
	 * a {@link MobiusJob#innerJoin(Dataset...)} job except that instead of calling
	 * <code>innerJoin</code>, simply change it to 
	 * <code>leftOuterJoin(Dataset, Dataset)</code>. 
	 * <p>
	 * 
	 * @param left left-hand side {@link Dataset}
	 * @param right right-hand side {@link Dataset}
	 * 
	 */
	public JoinOnConfigure leftOuterJoin(Dataset left, Dataset right)
		throws IOException
	{
		return this.leftOuterJoin(left, right, null);
	}
	
	
	/**
	 * Performing "Right Outer Join", the result contains all the records of
	 * the right {@linkplain Dataset} (the 2nd argument) with or without match 
	 * to the left {@linkplain Dataset}.
	 * <p>
	 * 
	 * If in a join group, there is no records from the right {@linkplain Dataset} 
	 * (the 2nd argument), by default, <code>null</code>(if the output format is 
	 * SequenceFileOutputFormat) or empty string (if the output format is 
	 * {@link TextOutputFormat}) is written for the selected columns from 
	 * the left {@linkplain Dataset}
	 * <p>
	 * 
	 * If <code>nullReplacement</code> is not null, then it will be used as 
	 * the value for the columns from the left dataset when no match in a 
	 * join group.
	 * <p>
	 * 
	 * To compose a <code>rightOuterJoin</code> is almost the same as composing
	 * a {@link MobiusJob#innerJoin(Dataset...)} job except that instead of calling
	 * <code>innerJoin</code>, simply change it to 
	 * <code>rightOuterJoin(Dataset, Dataset, Object)</code>. 
	 * <p>
	 * 
	 * @param left left-hand side {@link Dataset}
	 * @param right right-hand side {@link Dataset}
	 * @param nullReplacement the value to be used as the value for null columns, 
	 * it can be only the type supported by {@link Tuple}
	 */
	public JoinOnConfigure rightOuterJoin(Dataset left, Dataset right, Object nullReplacement)
		throws IOException
	{
		// leverage the leftOuterJoin by exchanging the position
		// of left and right dataset.
		return leftOuterJoin(right, left, nullReplacement);
	}
	
	
	
	/**
	 * Performing "Right Outer Join", the result contains all the records of
	 * the right {@linkplain Dataset} (the 2nd argument) with or without match 
	 * to the left {@linkplain Dataset}.
	 * <p>
	 * 
	 * If in a join group, there is no records from the right {@linkplain Dataset} 
	 * (the 2nd argument), by default, <code>null</code>(if the output format is 
	 * SequenceFileOutputFormat) or empty string (if the output format is 
	 * {@link TextOutputFormat}) is written for the selected columns from 
	 * the left {@linkplain Dataset}
	 * <p>
	 * 
	 * To compose a <code>rightOuterJoin</code> is almost the same as composing
	 * a {@link MobiusJob#innerJoin(Dataset...)} job except that instead of calling
	 * <code>innerJoin</code>, simply change it to 
	 * <code>rightOuterJoin(Dataset, Dataset)</code>. 
	 * <p>
	 * 
	 * @param left left-hand side {@link Dataset}
	 * @param right right-hand side {@link Dataset}
	 * @param nullReplacement the value to be used as the value for null columns, 
	 * it can be only the type supported by {@link Tuple}
	 */
	public JoinOnConfigure rightOuterJoin(Dataset left, Dataset right)
		throws IOException
	{
		return this.rightOuterJoin(left, right, null);
	}
	
	
	
	
	/**
	 * Perform inner join on the given <code>datasets</code>.
	 * <p>
	 * 
	 * The number of <code>datasets</code> must >= 2.
	 * One can join <b>more than two {@link Dataset} at once</b>
	 * only if the datasets have a shared key, i.e., they have
	 * columns that share the same meaning, the name of
	 * the columns don't have to be the same, but the content 
	 * (value) of the columns need to be the same.
	 * <p>
	 * 
	 * Form the performance perspective, the <b>biggest dataset
	 * </b> should be placed in the <b>right most side</b>.  The 
	 * <b>bigness</b> is measured in terms of values in a join 
	 * key, <b>NOT</b> by the total number of records of a dataset.
	 * <p>
	 * 
	 * Here is an example of how to create a inner join job:
	 * <pre>
	 * <code>
	 * public class MyJob extends MobiusJob
	 * {
	 * 	public void run(String[] args) throws Exception
	 * 	{
	 *		Dataset students = ...;
	 *		Dataset courses = ...;
	 * 
	 *		this
	 *		.innerJoin(students, courses)
	 *		.on( new EQ(new Column(students, "student_id"), new Column(courses, "student_id")) )
	 *		.save(this, new Path("$OUTPUT"),
	 *			new Column(students, "student_id"), 
	 *			new Column(students, "f_name"),
	 *			new Column(students, "l_name"),
	 *			new Column(courses, "c_title")
	 *		);
	 * 	}
	 * 	
	 * 	public static void main(String[] args) throws Exception
	 * 	{
	 * 		System.exit(MobiusJobRunner.run(new MyJob(), args));
	 * 	}
	 * }
	 * </code>
	 * </pre>
	 */
	public JoinOnConfigure innerJoin(Dataset... datasets)
	{	
		return new JoinOnConfigure(this.getConf(), datasets);
	}
	
	
	
	/**
	 * Start a group-by job.
	 * <p>
	 * 
	 * Group-by the given <code>aDataset</code> by
	 * certain column(s) (to be specified in the returned
	 * {@link GroupByConfigure}).
	 * <p>
	 * 
	 * Here is an example of group-by job:
	 * <pre>
	 * <code>
	 * public class MyJob extends MobiusJob
	 * {
	 * 	public void run(String[] args) throws Exception
	 * 	{
	 * 		.....
	 * 		this
	 * 		.group(order)
	 * 		.by(new Column(order, "order_person_id"))
	 * 		.save(this,
	 * 			new Path("$OUTPUT_PATH"),
	 * 			new Column(order, "order_person_id"),
	 * 			new Max(new Column(order, "order_id")));
	 * 	}
	 * 
	 * 	public static void main(String[] args) throws Exception
	 * 	{
	 * 		System.exit(MobiusJobRunner.run(new MyJob(), args));
	 * 	}
	 * }
	 * </code>
	 * </pre>
	 */
	public GroupByConfigure group(Dataset aDataset)
	{
		return new GroupByConfigure(this.getConf(), aDataset);
	}
	
	
	
	/**
	 * Performing a total sort on the aDataset.
	 * <p>
	 * 
	 * After the job has finished, concatenating 
	 * the out files together, the values in the files 
	 * are sorted according to the given {@link Sorter}.
	 * <p>
	 * 
	 * Here is an example of how to start a <code>sort</code>
	 * job:
	 * 
	 * <pre>
	 * <code>
	 * public MyJob extends MobiusJob
	 * {
	 * 	public void run(String[] args) throws Exception
	 * 	{
	 * 		.....
	 *		this
	 *		.sort(person)
	 *		.select(
	 *			new Column(ds, "age"),
	 *			new Column(ds, "gender"),
	 *			new Column(ds, "fname"),
	 *			new Column(ds, "lname"))
	 *		.orderBy(
	 *			new Sorter(new Column(ds, "age"), Ordering.ASC, true),
	 *			new Sorter(new Column(ds, "gender"), Ordering.DESC, true))
	 *		.save(
	 *			this,
	 *			new Path("$OUTPUT")
	 *		);
	 * 	}
	 * 
	 * 	public static void main(String[] args) throws Exception
	 * 	{
	 * 		System.exit(MobiusJobRunner.run(new MyJob(), args));
	 * 	}
	 * }
	 * </code>
	 * </pre>
	 */
	public SortProjectionConfigure sort(Dataset aDataset)
		throws IOException
	{
		return new SortProjectionConfigure(this.getConf(), aDataset);
	}
	
	
	protected FileSystem getFS()
	{
		if( this.fs==null )
		{
			try
			{
				this.fs = FileSystem.get(this.getConf());
			}catch(IOException e)
			{
				throw new RuntimeException(e);
			}
		}
		return this.fs;
	}
	
	
	void deleteTempFiles()
		throws IOException
	{
		LOGGER.info("Cleanning temporal files...");
		
		for(Path aTempFile:this.tempFiles )
		{
			if( !this.getFS().delete(aTempFile, true) )
			{
				LOGGER.warn("Cannot delete temp file:"+aTempFile.toString());
			}
			else
			{
				LOGGER.info(aTempFile.toString()+" deleted.");
			}
		}
		LOGGER.info("All temporal files are deleted.");
	}
	
	
	/**
	 * create an empty folder under hadoop.tmp.dir.
	 */
	public Path newTempPath()
		throws IOException
	{	
		Path tmp = new Path(this.getConf().get("hadoop.tmp.dir"), String.valueOf(System.currentTimeMillis()));
		while( this.getFS().exists(tmp) )
		{
			tmp = new Path(this.getConf().get("hadoop.tmp.dir"), String.valueOf(System.currentTimeMillis()));
		}
		
		if( !this.getFS().mkdirs(tmp) )
		{
			throw new IOException("Cannot create temp file:"+tmp.toString()+".");
		}
		
		// remember the temp file so it can be deleted after
		// this job has completed.
		this.tempFiles.add(tmp);
		
		return tmp;
	}
	
	
	
	/**
	 * Add a job, represented by the <code>aNewJob</code> object, into the execution queue.
	 * <p>
	 * 
	 * Users can use this method to add one or more jobs' configuration into the job queue, and Mobius engine
	 * will analyze the <code>aNewJob</code> objects within the queue to understand the dependence of jobs.  
	 * For example, if job B's input is from job A, then job B won't be submitted until A is completed 
	 * successfully.  If A failed, the B will not be submitted.
	 * <p>
	 *  
	 * 
	 * @param aNewJobConf a {@link Configuration} object represents a Hadoop job. 
	 * @throws IOException
	 */
	protected void addToExecQueue(Configuration aNewJobConf)
		throws IOException
	{
		// Add the new job into execution engine and realize
		// its dependency, if any.
		//
		// To realize the job dependency, we need to analyze the input
		// path of this new job.
		// 
		// The inputs of a job could be:
		// 1) if aNewJob is not a derived job (ex: result of another MR job), 
		// then the inputs of the job can be retrieved from "mapred.input.dir",
		// or from {@link MultipleInputs} (ex, joining different type of dataset)/
		// 2) if aNewJob is a derived job, the input  is from the output of previous
		// MR job.
		
		String inputFolders = aNewJobConf.get("mapred.input.dir", "");
		if ( inputFolders.length()==0 )
		{
			// the value of "mapred.input.dir" is empty, assuming the inputs of this job 
			// are coming from {@link MultipleInputs}.
			
			String multipleInputs = aNewJobConf.get(
					"mapred.input.dir.mappers"/* for using old MultipleInputs, v0.20.X */, 
					aNewJobConf.get("mapreduce.input.multipleinputs.dir.formats"/* for new MultipleInputs, v0.23.X */, ""));
			
			if( multipleInputs.length()>0 )
			{
				// the input paths of this job is coming from MultipleInputs, extract the input paths.
				// The format from {@link MultipleInputs} is like: hadoop_path1;corresponding_mapper1,hadoop_path2;corresponding_mapper2...
				String[] pathAndMapperPairs = multipleInputs.split (",");
				for( String aPair:pathAndMapperPairs )
				{
					String[] pathToMapper	= aPair.split (";");
					String path				= pathToMapper[0];
					String mapper			= pathToMapper[1];
					
					if ( inputFolders.length()==0 )
					{
						inputFolders = getPathOnly(path);
					}
					else
					{
						inputFolders = inputFolders + "," + getPathOnly(path);
					}
				}
			}
			else
			{
				throw new IllegalArgumentException("Cannot find input path(s) of job: ["+aNewJobConf.get("mapred.job.name")+"] from the following attributes: " +
						"mapred.input.dir, mapred.input.dir.mappers, nor mapreduce.input.multipleinputs.dir.formats. " +
						"Please specify the input path(s) of this job.");
			}
		}
		else
		{
			// the input path of this job is specified in mapred.input.dir
			inputFolders = getPathOnly(inputFolders);
		}
		
		
		
		////////////////////////////////////////////////////////////
		// validate output path of this job, to ensure it doesn't
		// use the same folder of another job's output.
		////////////////////////////////////////////////////////////
		String outputPath = aNewJobConf.get("mapred.output.dir", "");
		if( outputPath.isEmpty() )
			throw new IllegalStateException("Please specify the output directory of job:"+aNewJobConf.get("mapred.job.name"));
		
		if ( this.isOutputOfAnotherJob(outputPath) )
		{
			throw new IllegalArgumentException("Job [" + aNewJobConf.get ("mapred.job.name")+"]'s output ["+outputPath+"] is " +
					"the output of job["+jobTopology.get (outputPath).getJobName ()+"], " +
					"please make sure to use different output folder for each job.");
		}
		
		
		//////////////////////////////////////////////////////////////////
		// pass all the validation, start to build the dependencies.
		//////////////////////////////////////////////////////////////////
		Job newJob = new ConfigurableJob (new JobConf (aNewJobConf, this.getClass ()));
		
		newJob.setJobName (aNewJobConf.get ("mapred.job.name", aNewJobConf.get("mapreduce.job.name", "Mobius Job")));
		for ( String anInputOfNewJob : inputFolders.split (",") )
		{
			// Added to track inputs for local PC sampling
			inputPaths.add(anInputOfNewJob);
			
			Job dependsOn = jobTopology.get (this.getFS().makeQualified(new Path(anInputOfNewJob)).toUri());
			if ( dependsOn != null )
			{
				List<Job> dependingJobs = newJob.getDependingJobs ();

				boolean alreadyInDependency = dependingJobs != null && dependingJobs.contains (dependsOn);
				if ( alreadyInDependency )
				{
					// already added, do nothing.
				}
				else
				{
					LOGGER.info (newJob.getJobName () + " depends on " + dependsOn.getJobName ());
					newJob.addDependingJob (dependsOn);
				}
			}
		}
		
		// put the output of this <code>newJob</code> into job topology
		// so that later if a job read this <code>newJob</code>'s output
		// as its input, then the system can detect the dependency.

		URI outputPathURI = this.getFS().makeQualified(new Path(outputPath)).toUri();
		LOGGER.info("Adding Job:"+newJob.getJobName()+"\tOutput:["+outputPath.toString()+"]");
		jobTopology.put (outputPathURI, newJob);
	}
	
	
	
	/**
	 * returning only the "path" part of the input URI.
	 */
	protected String getPathOnly(String uriStr)
	{
		try 
		{
			URI uri = new URI(uriStr);
			return uri.getPath();
		}
		catch (URISyntaxException e) 
		{
			LOGGER.error(e);
			throw new IllegalArgumentException(e);
		}
	}

}
