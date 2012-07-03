package com.ebay.erl.mobius.core.function;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.math.BigDecimal;

import org.junit.Test;

import com.ebay.erl.mobius.core.MobiusTestJob;
import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.model.Column;
import com.ebay.erl.mobius.core.model.Tuple;

public class SumTest extends MobiusTestJob{
	
	private static final long serialVersionUID = 1986065117638035797L;

	@Test
	public void testOverflow()
		throws IOException
	{
		Dataset ds = this.createDummyDataset("ds", new String[]{"COLUMN"});
		
		BigDecimal expected = BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.valueOf(Double.MAX_VALUE));
		
		Tuple t = new Tuple();
		t.put("COLUMN", Double.MAX_VALUE);
		
		Sum func = new Sum(new Column(ds, "COLUMN"));
		func.reset();
		func.consume(t);
		func.consume(t);
		
		BigDecimal actual = (BigDecimal)func.getComputedResult().get(0);
		
		assertEquals(0, actual.compareTo(expected));
	}
	
	@Test
	public void test1()
		throws IOException
	{
		Dataset ds = this.createDummyDataset("ds", new String[]{"COLUMN"});
		BigDecimal expected = BigDecimal.valueOf(-5D);
		
		Tuple t1 = new Tuple();
		t1.put("column", 10D);
		
		Tuple t2 = new Tuple();
		t2.put("column", Double.MIN_VALUE);
		
		Tuple t3 = new Tuple();
		t3.put("column", Math.abs(Double.MIN_VALUE));
		
		Tuple t4 = new Tuple();
		t4.put("column", -15D);
		
		Sum func = new Sum(new Column(ds, "COLUMN"));
		func.reset();
		
		func.consume(t1);
		func.consume(t2);
		func.consume(t3);
		func.consume(t4);
		
		BigDecimal actual = (BigDecimal)func.getComputedResult().get(0);
		
		assertEquals(0, actual.compareTo(expected));
	}
}
