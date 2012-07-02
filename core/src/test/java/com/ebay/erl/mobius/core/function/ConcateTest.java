package com.ebay.erl.mobius.core.function;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.ebay.erl.mobius.core.MobiusTestJob;
import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;

public class ConcateTest extends MobiusTestJob 
{
	private static final long serialVersionUID = 9178101309265095838L;

	@Test
	public void test()
		throws IOException
	{
		Dataset ds = this.createDummyDataset("test", new String[]{"ID"});
		
		Concate func = new Concate(new Column(ds, "ID"), ",");
		
		List<Tuple> tuples		= new ArrayList<Tuple>();
		StringBuffer trueAnswer = new StringBuffer();
		final int max = 50;
		for( int i=0;i<max;i++)
		{
			Tuple t = new Tuple();
			t.put("ID", i);
			tuples.add(t);
			
			trueAnswer.append(i);
			if( i<max-1)
			{
				trueAnswer.append(",");
			}
		}
		
		func.reset();
		for( Tuple t:tuples )
		{
			func.consume(t);
		}
		
		String computedResult = func.getComputedResult().toString();
		assertEquals(trueAnswer.toString(), computedResult);
	}
}
