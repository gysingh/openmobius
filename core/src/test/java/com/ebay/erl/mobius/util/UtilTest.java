package com.ebay.erl.mobius.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.ebay.erl.mobius.core.collection.BigTupleList;
import com.ebay.erl.mobius.core.model.Tuple;

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
public class UtilTest 
{
	@SuppressWarnings("unchecked")
	@Test
	public void testCrossProduct()
		throws IOException
	{
		List<Tuple> ds1 = this.generate(2, new String[]{"A", "B"});
		
		List<Tuple> ds2 = this.generate(3, new String[]{"AA", "BB", "CC"});
		
		List<Tuple> ds3 = this.generate(5, new String[]{"X"});
		
		// the cross product should be 30 rows
		
		BigTupleList result = (BigTupleList)Util.crossProduct(null, null, ds1, ds2, ds3);
		
		Assert.assertEquals(30, result.size());
		
		Tuple first = new Tuple();
		first
			.put("a", "A_1")
			.put("aa", "AA_1")
			.put("B", "B_1")
			.put("bb", "BB_1")
			.put("cc", "CC_1")
			.put("X", "X_1");
		
		Tuple last = new Tuple();
		last
			.put("a", "A_2")
			.put("aa", "AA_3")
			.put("B", "B_2")
			.put("bb", "BB_3")
			.put("cc", "CC_3")
			.put("X", "X_5");
		
		int idx = 0;
		for( Tuple t:result )
		{
			if( idx==0) // first one
				Assert.assertEquals(first, t);
			if( idx==29 )// last one
				Assert.assertEquals(last, t);
			idx++;
		}		
	}
	
	
	protected List<Tuple> generate(int rows, String... keys)
	{
		List<Tuple> result = new ArrayList<Tuple>();
		for( int i=0;i<rows;i++ )
		{
			Tuple t = new Tuple();
			for(String aKey:keys)
			{
				t.put(aKey, aKey+"_"+(i+1));
			}
			result.add(t);
		}
		return result;
	}
}
