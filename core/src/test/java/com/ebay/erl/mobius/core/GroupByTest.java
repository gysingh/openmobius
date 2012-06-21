package com.ebay.erl.mobius.core;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.builder.TSVDatasetBuilder;
import com.ebay.erl.mobius.core.function.Counts;
import com.ebay.erl.mobius.core.function.Max;
import com.ebay.erl.mobius.core.model.Column;

/**
 * <p>
 * This product is licensed under the Apache License,  Version 2.0, 
 * available at http://www.apache.org/licenses/LICENSE-2.0.
 * 
 * This product contains portions derived from Apache hadoop which is 
 * licensed under the Apache License, Version 2.0, available at 
 * http://hadoop.apache.org.
 * 
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Neel Sundaresan
 *
 */
public class GroupByTest extends MobiusJob
{
	private static final long serialVersionUID = -5877391996762185731L;
	
	private Log LOGGER = LogFactory.getLog(GroupByTest.class);
	
	@Override
	public int run(String[] args) throws Exception {
		
		String input	= args[0];
		String output	= args[1];
		
		LOGGER.info("Input Path:"+input);
		LOGGER.info("Output Path:"+output);
		
		Dataset items = TSVDatasetBuilder.newInstance(this, "items_table", new String[]{"ITEM_ID", "SELLER_ID", "BUYER_ID", "ITEM_PRICE"})
			.addInputPath(new Path(input))
			.build();
		
		this.group(items)
			.by("SELLER_ID")			
			.save(this, 
					new Path(output), 
					new Column(items, "SELLER_ID"),
					new Counts(new Column(items, "ITEM_ID")),
					new Max(new Column(items, "ITEM_PRICE"))
			
		);
		
		
		return 0;
	}
	
	@Test
	public void test()
	{
		try
		{			
			File input	= new File(this.getClass().getResource("/com/ebay/erl/mobius/core/items.tsv").toURI());
			File output	= new File(System.getProperty("java.io.tmpdir"), "output");
			
			
			File trueAnswer = new File(this.getClass().getResource("/com/ebay/erl/mobius/core/groupby.true.answer").toURI());
			
			String[] args	= new String[]{input.getAbsolutePath(), output.getAbsolutePath()};
			
			int exitCode = MobiusJobRunner.run(new GroupByTest(), args);
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
