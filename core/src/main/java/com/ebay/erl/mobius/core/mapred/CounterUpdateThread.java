package com.ebay.erl.mobius.core.mapred;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Counter;

/**
 * Responsible for updating the Hadoop counters
 * in background.
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
class CounterUpdateThread implements Runnable
{
	private boolean run;
	
	private Map<Counter, MutableLong> counts;
	
	private Map<String, Counter> str_to_counter;
	
	private Reporter r;
	
	private static final long _SLEEP = 10L*1000L;// sleep 10 seconds
	
	public CounterUpdateThread(Reporter r)
	{
		this.counts = new HashMap<Counter, MutableLong>();
		this.str_to_counter = new HashMap<String, Counter>();
		this.r = r;
	}

	@Override
	public void run()
	{
		while( this.run )
		{
			try
			{	
				this.reportCounters ();
				Thread.sleep (_SLEEP);
			}
			catch(Throwable t)
			{
				throw new RuntimeException(t);
			}
		}
	}
	
	private void reportCounters()
	{
		synchronized(counts)
		{
			for(Counter aCounter:counts.keySet () )
			{
				long previous = aCounter.getValue ()<0?0L:aCounter.getValue ();
				long current = counts.get (aCounter).longValue();
				
				long diff = current-previous;
				if( diff!=0 )
				{
					this.r.setStatus ("Updating counters on "+new Date());
					this.r.progress();// set the progress flag so Hadoop know this is still alive.
					aCounter.increment (diff);
				}
			}
		}
	}
	
	private Counter getCounterByName(String groupName, String counterName)
	{
		Counter c;
		if( (c=this.str_to_counter.get (groupName+counterName))==null )
		{
			c = this.r.getCounter (groupName, counterName);
			this.str_to_counter.put (groupName+counterName, c);
		}
		return c;
	}
	
	public void updateCounter(String groupName, String counterName, long newCounts)
	{
		Counter counter = this.getCounterByName (groupName, counterName);
		synchronized(this.counts)
		{
			MutableLong count;
			if( (count=this.counts.get (counter))==null )
			{
				count = new MutableLong(0L);
				this.counts.put (counter, count);
			}
			count.setValue (newCounts);
		}
	}
	
	public void stop()
	{
		this.run = false;
		this.reportCounters ();
	}
}
