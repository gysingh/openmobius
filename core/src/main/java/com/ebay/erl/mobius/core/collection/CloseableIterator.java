package com.ebay.erl.mobius.core.collection;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An {@link Iterator} with {@link Closeable}.
 * 
 * 
 * 
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
 * @param <E>
 */
public interface CloseableIterator<E> extends Iterator<E>, Closeable 
{
	/**
	 * {@inheritDoc}
	 * <p>
	 * 
	 * If the elements of this iterator has not been
	 * all iterated over, but the user exit the iterating
	 * loop, this method <b>MUST BE CALLED</b> to release
	 * any system resources associated with it.
	 */
	@Override
	public void close();
}
