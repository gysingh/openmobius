package com.ebay.erl.mobius.core;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.builder.TSVDatasetBuilder;
import com.ebay.erl.mobius.core.sort.Sorter;
import com.ebay.erl.mobius.core.sort.Sorter.Ordering;


/**
 * <p>
 * This product is licensed under the Apache License,  Version 2.0, 
 * available at http://www.apache.org/licenses/LICENSE-2.0.
 * 
 * This product contains portions derived from Apache hadoop which is 
 * licensed under the Apache License, Version 2.0, available at 
 * http://hadoop.apache.org.
 * 
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Jack Shen, Gyanit Singh, Neel Sundaresan
 *
 */
public class SortTest extends MobiusJob
{
	private static final long serialVersionUID = 7945209428566552518L;
	
	

	@Override
	public int run(String[] args)
		throws Exception 
	{	
		Dataset items = TSVDatasetBuilder.newInstance(this, "items_table", new String[]{"ITEM_ID", "SELLER_ID", "BUYER_ID", "ITEM_PRICE"})
			.addInputPath(new Path(args[0]))
		.build();
		
		this.sort(items)
			.select("ITEM_ID", "ITEM_PRICE", "SELLER_ID", "BUYER_ID")
			.orderBy(
					new Sorter("ITEM_PRICE", Ordering.DESC, true),
					new Sorter("SELLER_ID", Ordering.ASC) 
			).save(this, new Path(args[1]));
		
		return 0;
	}
	
	
	@Test
	public void test()
	{
		try
		{			
			File input	= new File(this.getClass().getResource("/com/ebay/erl/mobius/core/items.tsv").toURI());
			File output	= new File(System.getProperty("java.io.tmpdir"), "output");
			
			
			File trueAnswer = new File(this.getClass().getResource("/com/ebay/erl/mobius/core/sort.true.answer").toURI());
			
			String[] args	= new String[]{input.getAbsolutePath(), output.getAbsolutePath()};
			
			int exitCode = MobiusJobRunner.run(new SortTest(), args);
			if( exitCode==0 )
			{
				assertTrue("Generated result doesn't match true answer.", TestUtil.equalFile(output, trueAnswer));
			}
			else
			{
				throw new RuntimeException("Non zero exit code:"+exitCode);
			}
		}catch(Throwable e)
		{
			e.printStackTrace();
			fail();
		}
	}

}
