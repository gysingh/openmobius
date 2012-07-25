package com.ebay.erl.mobius.core.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;

import com.ebay.erl.mobius.core.criterion.TupleCriterion;
import com.ebay.erl.mobius.core.model.Tuple;

public class TupleCollector
{
	private OutputCollector<WritableComparable<?>, WritableComparable<?>> output;
	
	private final String[] keyColumns;
	
	private final String[] valueColumns;
	
	private final boolean mapOnly;
	
	private TupleCriterion tupleCriteria;
	
	private Configuration conf;
	
	public TupleCollector(OutputCollector<WritableComparable<?>, WritableComparable<?>> output, String[] keyColumns, String[] valueColumns, boolean mapOnly, TupleCriterion tupleCriteria, Configuration conf)
	{
		this.output			= output;
		this.keyColumns 	= keyColumns;
		this.valueColumns	= valueColumns;
		this.mapOnly		= mapOnly;
		this.tupleCriteria	= tupleCriteria;
		this.conf			= conf;
	}
	
	public void write(Tuple tuple)
		throws IOException 
	{
		
		Tuple value = new Tuple();
		for( String aValueColumn:valueColumns )
		{
			value.insert(aValueColumn, tuple.get(aValueColumn));
		}
		
		if( mapOnly )
		{
			if( this.tupleCriteria!=null && !this.tupleCriteria.accept(value, this.conf)) 
			{
				return;
			}
			
			output.collect(NullWritable.get(), value);
		}
		else
		{
			Tuple key = new Tuple();
			for( String aKeyColumn:keyColumns )
			{
				key.insert(aKeyColumn, tuple.get(aKeyColumn));
			}
			
			if( this.tupleCriteria!=null && !this.tupleCriteria.accept(value, this.conf)) 
			{
				return;
			}
			
			output.collect(key, value);
		}
	}
}
