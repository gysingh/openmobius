package com.ebay.erl.mobius.core.function;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.ebay.erl.mobius.core.MobiusTestJob;
import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.model.Array;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.ResultWrapper;
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
	
	
	@SuppressWarnings("unchecked")
	@Test
	public void test2()
		throws IOException
	{
		// test values that has wrapped results.
		List<Tuple> tuples = new ArrayList<Tuple>();
		
		Array a1 = new Array();
		a1.add(1);
		a1.add(2);
		a1.add(3);		
		Tuple t1 = new Tuple();
		t1.put("C1", new ResultWrapper(a1));		
		tuples.add(t1);
		
		Tuple t2 = new Tuple();
		t2.put("C1", 4);
		tuples.add(t2);
		
		Array a2 = new Array();
		a2.add(5);		
		Tuple t3 = new Tuple();
		t3.put("C1", a2);
		tuples.add(t3);
		
		
		Dataset ds = this.createDummyDataset("test", new String[]{"C1"});
		Concate func = new Concate(new Column(ds, "C1"));
		func.reset();
		for( Tuple t:tuples )
		{
			func.consume(t);
		}
		
		String expectedResult = "1;2;3;4;5";
		String actualResult = func.getComputedResult().toString();
		
		assertEquals(expectedResult, actualResult);
	}
}
