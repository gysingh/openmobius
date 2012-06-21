package com.ebay.erl.mobius.core.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;



public class ResultWrapper<R> implements Writable
{
	private R combinedResult;
	
	public ResultWrapper(){}
	
	public ResultWrapper(R combinedResult)
	{
		this.combinedResult = combinedResult;
	}

	public R getCombinedResult()
	{
		return this.combinedResult;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException 
	{
		byte combinedResultType = in.readByte();
		
		List<Object> result = new ArrayList<Object>();
		ReadFieldImpl reader = new ReadFieldImpl(result, in, null);
		
		reader.handle(combinedResultType);
		
		if( result.size()>0 )
			this.combinedResult = (R)result.get(0);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		byte type = Tuple.getType(this.combinedResult);
		out.write(type);
		
		WriteImpl writer = new WriteImpl(out);
		writer.setValue(this.combinedResult);
		writer.handle(type);
	}
}
