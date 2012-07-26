package com.ebay.erl.mobius.core;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;

import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.builder.TSVDatasetBuilder;

public class MobiusTestJob extends MobiusJob
{
	private static final long serialVersionUID = 5691278392429754294L;

	protected Dataset createDummyDataset(String name, String[] schema)
		throws IOException
	{
		File input = null;
		try 
		{
			input = new File(this.getClass().getResource("/com/ebay/erl/mobius/core/items.tsv").toURI());
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
		
		// creating a dummy dataset, the schema doesn't match the actual file schema,
		// it's just for unit testing.
		Dataset dummy = TSVDatasetBuilder.newInstance(this, name, schema)
			.addInputPath(new Path(input.getAbsolutePath()))
			.build();
		
		return dummy;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// do nothing
		return 0;
	}

}
