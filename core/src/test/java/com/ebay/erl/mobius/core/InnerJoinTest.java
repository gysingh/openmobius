package com.ebay.erl.mobius.core;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.ebay.erl.mobius.core.JoinOnConfigure.EQ;
import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.builder.TSVDatasetBuilder;
import com.ebay.erl.mobius.core.function.Counts;
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
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Jack Shen, Gyanit Singh, Neel Sundaresan
 *
 */
public class InnerJoinTest extends MobiusJob
{
	private static final long serialVersionUID = -5484412204407793014L;

	@Override
	public int run(String[] args)
		throws Exception 
	{
		
		Dataset items = TSVDatasetBuilder.newInstance(this, "items_table", new String[]{"ITEM_ID", "SELLER_ID", "BUYER_ID", "ITEM_PRICE"})
			.addInputPath(new Path(args[0]))
		.build();
		
		Dataset members = TSVDatasetBuilder.newInstance(this, "members_table", new String[]{"ID", "NAME"})
			.addInputPath(new Path(args[1]))
			.setDelimiter(",")
		.build();
		
		this.innerJoin(members, items)
			.on(new EQ(new Column(members, "ID"), new Column(items, "BUYER_ID")))
			.save(this, new Path(args[2]), 
				new Column(items, "BUYER_ID"),
				new Column(members, "NAME"),
				new Counts(new Column(items, "ITEM_ID"))
		);
		
		return 0;
	}

	@Test
	public void test()
	{
		try
		{			
			File input1	= new File(this.getClass().getResource("/com/ebay/erl/mobius/core/items.tsv").toURI());
			File input2	= new File(this.getClass().getResource("/com/ebay/erl/mobius/core/members.csv").toURI());
			File output	= new File(System.getProperty("java.io.tmpdir"), "output");
			
			File trueAnswer = new File(this.getClass().getResource("/com/ebay/erl/mobius/core/innerjoin.true.answer").toURI());
			
			String[] args	= new String[]{input1.getAbsolutePath(), input2.getAbsolutePath(), output.getAbsolutePath()};
			
			int exitCode = MobiusJobRunner.run(new InnerJoinTest(), args);
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
