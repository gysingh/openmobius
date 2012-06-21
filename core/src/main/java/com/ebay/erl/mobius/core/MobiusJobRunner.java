package com.ebay.erl.mobius.core;

import java.io.IOException;
import java.util.Collection;
import java.util.Observable;
import java.util.Observer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ebay.erl.mobius.core.criterion.TupleRestrictions;
import com.ebay.erl.mobius.util.JVMShutdownNotifier;

/**
 * Executes a {@link MobiusJob}.
 * <p>
 * 
 * When executing a Mobius job, use this class to ensure that 
 * Mobius submits the job properly. Using {@link ToolRunner}
 * to submit a {@link MobiusJob} will fail.
 * <p>
 * 
 * Here is an example:
 * <pre>
 * <code>
 * public class MyJob extends MobiusJob
 * {
 * 	public void run()
 * 		throws Exception
 * 	{
 * 		....
 * 		// your flow here
 * 		....
 * 	}
 * 
 * 	public static void main(String[] args)
 * 		throws Throwable	
 * 	{
 * 		int exitCode = MobiusJobRunner.run(new MyJob(), args);
 * 		System.exit(exitCode);
 * 	}
 * }
 * </code>
 * </pre>
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
public class MobiusJobRunner extends ToolRunner
{
	private static final Log LOGGER = LogFactory.getLog (MobiusJobRunner.class);
	
	
	/**
	 * Submit the <code>tool</code> with the specified <code>conf</code> and <code>args</code>.
	 * <p>
	 * 
	 * <code>tool</code> can be {@link MobiusJob} or any instance of {@link org.apache.hadoop.util.Tool}.
	 * If <code>tool</code> is an instance of {@link MobiusJob}, it will be submitted using
	 * {@link JobControl}.  If it's not an instance of {@link MobiusJob}, then it will be submitted
	 * using <code>ToolRunner.run(conf, tool, args)</code> directly.
	 * 
	 */
	public static int run(Configuration conf, Tool tool, String[] args)
		throws Exception
	{
		if( tool instanceof MobiusJob )
		{
			MobiusJob mobiusJob = (MobiusJob)tool;
			
			int exit;
			try
			{
				exit = ToolRunner.run(conf, tool, args);
			}catch(Throwable t)
			{
				t.printStackTrace();
				exit = 1;
			}
			
			if( exit==0 )
			{
				// setup correctly
			
				JobControl control = new JobControl("Mobius Job ["+tool.getClass ().getCanonicalName () +"]");
				
				Collection<Job> allJobs = mobiusJob.jobTopology.values ();
				control.addJobs (allJobs);
				
				LOGGER.info(allJobs.size()+" Hadoop job(s) to run.");
				
				Thread t = new Thread(control);
				t.start ();
				
				StatusCheckingThread statusChecking = new StatusCheckingThread(tool, allJobs, control);			
				statusChecking.start ();
				
				statusChecking.join ();
				LOGGER.info(" All job(s) done.");
				
				statusChecking.complete();
				
				int exitCode = control.getFailedJobs ().size ()==0?0:1;
				
				mobiusJob.deleteTempFiles();
				return exitCode;
			}
			else
			{
				mobiusJob.deleteTempFiles();
				return exit;
			}
			
		}
		else
		{
			return ToolRunner.run(conf, tool, args);
		}
	}
	
	/**
	 * submit the <code>tool</code> with specified <code>args</code>.
	 */
	public static int run(Tool tool, String[] args) throws Exception
	{
		Configuration conf = tool.getConf ()==null?new Configuration():tool.getConf ();
		
		LOGGER.info ("file.file.impl:"+conf.get ("fs.file.impl"));
		
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		conf = gop.getConfiguration ();
		String[] remaining = gop.getRemainingArgs ();
		TupleRestrictions.configure (conf);

		return MobiusJobRunner.run (conf, tool, remaining);
	}
	
	
	
	private static String jobToString(Job aJob )
	{
		StringBuffer sb = new StringBuffer ();
		sb.append ("job mapred id:\t").append (aJob.getAssignedJobID () == null ? "unassigned" : aJob.getAssignedJobID ().toString ()).append ("\t");
		sb.append ("job name: ").append (aJob.getJobName ()).append ("\n");
		String state = "Unset";
		switch( aJob.getState () )
		{
			case Job.DEPENDENT_FAILED:
				state = "DEPENDENT_FAILED";
				break;
			case Job.FAILED:
				state = "FAILED";
				break;
			case Job.READY:
				state = "READY";
				break;
			case Job.RUNNING:
				state = "RUNNING";
				break;
			case Job.SUCCESS:
				state = "SUCCESS";
				break;
			case Job.WAITING:
				state = "WAITING";
				break;
		}
		
		sb.append ("job state:\t").append (state).append ("\n");
		
		sb.append ("job id:\t").append (aJob.getJobID ()).append ("\n");
		
		
		sb.append ("job message:\t").append (aJob.getMessage ()).append ("\n");
		
		
//		comment out on March 30, 2012.  As NPE is thrown on Apollo.
//
//		if ( aJob.getDependingJobs () == null || aJob.getDependingJobs ().size () == 0 )
//		{
//			sb.append ("job has no depending job:\t").append ("\n");
//		} else
//		{
//			sb.append ("job has ").append (aJob.getDependingJobs ().size ()).append (" dependeng jobs:\n");
//			for ( int i = 0; i < aJob.getDependingJobs ().size (); i++ )
//			{
//				sb.append ("\t depending job ").append (i).append (":\t");
//				sb.append ((aJob.getDependingJobs ().get (i)).getJobName ()).append ("\n");
//			}
//		}
		return sb.toString ().trim ();
	}
	
	
	
	private static class StatusCheckingThread extends Thread implements Observer
	{		
		public StatusCheckingThread(Tool tool, Collection<Job> allJobs, JobControl control) 
		{
			this.tool = tool;
			this.allJobs = allJobs;
			this.control = control;
			JVMShutdownNotifier.getInstance().addObserver(this);
		}
		
		private Tool tool;
		private Collection<Job> allJobs;
		final JobControl control;
		
		private volatile boolean keepRunning = true;
		
		private RunningJob getRunningJob(Job aJob)		
		{
			try
			{
				JobClient client = new JobClient(new JobConf(tool.getConf ()));
				RunningJob runningJob = client.getJob (aJob.getAssignedJobID ());
				client.close ();
				return runningJob;
			}
			catch(IOException e)
			{
				if( e.getMessage ().toLowerCase ().indexOf ("reset by peer")>=0 )
				{
					try
					{
						LOGGER.warn("Connection reset, retry in 5 seconds..");
						Thread.sleep (5000); 
					}
					catch ( InterruptedException e1 ){ e1.printStackTrace(); }
					
					return getRunningJob(aJob);
				}
				else
				{
					throw new RuntimeException(e);
				}
			}
		}
		
		
		public void complete()
		{
			this.keepRunning = false;
		}
		
		@Override
		public void run()
		{
			final int totalJobs = allJobs.size();
			
			String previousStatus = null;
			
			while( keepRunning )
			{
				try
				{
					Thread.sleep (10*1000);
					LOGGER.info("Jobs status monitor thread heartbeat.");
				}
				catch ( InterruptedException e )
				{
					throw new RuntimeException(e);
				}
				
				StringBuffer currentStatus = new StringBuffer();
				for( Job aJob:allJobs )
				{							
					float mapProgress		= -1F;
					float reduceProgress 	= -1F;
					if( aJob.getAssignedJobID ()!=null )
					{
						try
						{	
							RunningJob runningJob = getRunningJob (aJob);
							if( runningJob!=null )
							{
								mapProgress = runningJob.mapProgress ();
								reduceProgress = runningJob.reduceProgress ();
							}
						}
						catch ( IOException e )
						{
							e.printStackTrace ();
						}										
					}
					
					currentStatus.append(jobToString(aJob)).append("\n");
					currentStatus.append("job mapper progress:\t"+(mapProgress*100F)).append("\n");
					currentStatus.append("job reducer progress:\t"+(reduceProgress*100F)).append("\n");
					currentStatus.append("\n");
				}
				
				if( previousStatus==null || !previousStatus.equals(currentStatus.toString()) )
				{
					// status has changed					
					LOGGER.info("\n"+currentStatus);						
					previousStatus = currentStatus.toString();
				}
				
				
				
				if( control.getFailedJobs ().size ()!=0 )
				{
					// has failed job
					for(Job aFailedJob:control.getFailedJobs ())
					{
						String status = "N/A";
						switch( aFailedJob.getState () )
						{
							case Job.DEPENDENT_FAILED:
								status = "DEPENDENT_FAILED";
								break;
							case Job.FAILED:
								status = "FAILED";
								break;
						}
						LOGGER.warn(aFailedJob.getJobName ()+"("+aFailedJob.getAssignedJobID ()+") "+status);
					}
					break;
				}
				
				if( control.getSuccessfulJobs ().size ()==totalJobs )
				{
					// all job completed
					break;
				}
			}
		}

		@Override
		public void update(Observable o, Object arg) 
		{
			LOGGER.info("shutting down job monitor");
			this.keepRunning = false;
		}
	}
}
