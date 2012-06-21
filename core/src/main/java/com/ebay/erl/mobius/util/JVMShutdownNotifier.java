package com.ebay.erl.mobius.util;

import java.util.Observable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Observing JVM shutdown signal and notify
 * objects that register to this when a shutdown
 * signal happened.
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
 */
public class JVMShutdownNotifier extends Observable implements Runnable 
{
	private static final JVMShutdownNotifier _INSTANCE = new JVMShutdownNotifier();
	
	private static final Log LOGGER = LogFactory.getLog (JVMShutdownNotifier.class);
	
	private JVMShutdownNotifier()
	{
		///////////////////////////////////
		// setup JVM termination Hookup
		///////////////////////////////////
		Runtime.getRuntime().addShutdownHook(new Thread(this));
	}
	
	public static JVMShutdownNotifier getInstance()
	{
		return JVMShutdownNotifier._INSTANCE;
	}
	
	@Override
	public void run() 
	{
		// JVM shutdown detected, notify objects
		LOGGER.info("JVM shutdown signal detected");
		this.setChanged();
		this.notifyObservers();
	}
}
