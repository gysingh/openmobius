package com.ebay.erl.mobius.core.function;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.ebay.erl.mobius.core.MobiusJob;
import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.builder.TSVDatasetBuilder;
import com.ebay.erl.mobius.core.model.Tuple;
import com.ebay.erl.mobius.core.sort.Sorter;
import com.ebay.erl.mobius.core.sort.Sorter.Ordering;
import com.ebay.erl.mobius.util.SerializableUtil;

public class TopTest extends MobiusJob{

	@Test
	public void testSorters()
		throws IOException, URISyntaxException
	{
		
		File input	= new File(this.getClass().getResource("/com/ebay/erl/mobius/core/items.tsv").toURI());
		
		// creating a dummy dataset, the schema doesn't match the actual file schema,
		// it's just for unit testing.
		Dataset dummy = TSVDatasetBuilder.newInstance(this, "dummy", new String[]{"COUNTS", "ID"})
			.addInputPath(new Path(input.getAbsolutePath()))
			.build();
		
		final int topK = 5;
		
		Top topFunction = new Top(
				dummy, 
				new String[]{"COUNTS", "ID"},
				new Sorter[]{new Sorter("COUNTS", Ordering.ASC, true)}, 
				topK
		);
		
		topFunction.reset();
		
		int size = 500;
		int[] counts = new int[size];
		
		for( int i=size-1;i>=0;i-- )
		{
			int count = (int)(Math.random()*1000);
			counts[i] = count;
			
			Tuple t = new Tuple();
			t.put("COUNTS", count);
			t.put("ID", "ID_"+i);
			
			topFunction.consume(t);
		}
		
		Arrays.sort(counts);
		
		// the counts is sorted in ascending order, so
		// topK element is in the tail of the "counts"
		// array.
		int k = 0;
		for( Tuple t: topFunction.getResult() )
		{
			k++;
			assertEquals(counts[500-k], t.getInt("TOP_COUNTS"));
		}
		
		
		
		//////////////////////////////////////////////////////////////
		// serialize Top and deserialize it back, then test
		//////////////////////////////////////////////////////////////
		topFunction = (Top)SerializableUtil.deserializeFromBase64(SerializableUtil.serializeToBase64(topFunction), null);
		topFunction.reset();
		for( int i=size-1;i>=0;i-- )
		{
			int count = (int)(Math.random()*1000);
			counts[i] = count;
			
			Tuple t = new Tuple();
			t.put("COUNTS", count);
			t.put("ID", "ID_"+i);
			
			topFunction.consume(t);
		}
		Arrays.sort(counts);
		
		k = 0;
		for( Tuple t: topFunction.getResult() )
		{
			k++;
			assertEquals(counts[500-k], t.getInt("TOP_COUNTS"));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// do nothing
		return 0;
	}
}
