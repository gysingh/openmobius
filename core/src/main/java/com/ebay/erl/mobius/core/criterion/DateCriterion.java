package com.ebay.erl.mobius.core.criterion;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

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
class DateCriterion extends AtomicCriterion
{
	private static final long serialVersionUID = -2129702838017686079L;

	private SimpleDateFormat formatter;
	
	private long userSpecifiedTimeInMillis;
	private boolean comparingYYYYMMDDOnly = true;
	
	DateCriterion(String columnName, String columnFormat, long userSpecifiedTimeInMillis, RelationalOperator op)
	{
		super (columnName, userSpecifiedTimeInMillis, op);
		
		
		this.userSpecifiedTimeInMillis = userSpecifiedTimeInMillis;
		
		Calendar c = Calendar.getInstance ();
		c.setTimeInMillis (userSpecifiedTimeInMillis);
		
		comparingYYYYMMDDOnly = 
			c.get (Calendar.HOUR_OF_DAY)==0 && 
			c.get (Calendar.MINUTE)==0 &&
			c.get (Calendar.SECOND)==0 &&
			c.get (Calendar.MILLISECOND)==0;
		
		if( columnFormat==null )
		{
			// use default format
			if (comparingYYYYMMDDOnly)
			{
				columnFormat = "yyyy-MM-dd";
			}
			else
			{
				columnFormat = "yyyy-MM-dd HH:mm:ss";
			}
		}
		
		this.formatter = new SimpleDateFormat(columnFormat);
	}
	
	private long getCurrentValue(Tuple t)
	{
		Calendar target = Calendar.getInstance ();
		try
		{
			Object value = t.get(columnName);
			if( value instanceof Calendar)
			{
				target.setTimeInMillis(((Calendar)value).getTimeInMillis());
			}
			else if ( value instanceof java.util.Date )
			{
				target.setTimeInMillis( ((java.util.Date)value).getTime() );
			}
			else
			{
				target.setTime (this.formatter.parse (t.getString(columnName)));
			}
			
			if( this.comparingYYYYMMDDOnly )
			{
				target.set (Calendar.HOUR_OF_DAY, 0);
				target.set (Calendar.MINUTE, 0);
				target.set (Calendar.SECOND, 0);
				target.set (Calendar.MILLISECOND, 0);
			}
			
			return target.getTimeInMillis ();
		}
		catch ( ParseException e )
		{
			throw new RuntimeException(e);
		}		
	}

	@Override
	protected boolean eq(Tuple currentRow)
	{
		return this.getCurrentValue (currentRow)==this.userSpecifiedTimeInMillis;
	}
	
	@Override
	protected boolean ne(Tuple currentRow)
	{
		return this.getCurrentValue (currentRow)!=this.userSpecifiedTimeInMillis;
	}
	
	@Override
	protected boolean ge(Tuple currentRow)
	{
		return this.getCurrentValue (currentRow) >= this.userSpecifiedTimeInMillis;
	}
	
	@Override
	protected boolean gt(Tuple currentRow)
	{
		return this.getCurrentValue (currentRow) > this.userSpecifiedTimeInMillis;
	}
	
	@Override
	protected boolean le(Tuple currentRow)
	{
		return this.getCurrentValue (currentRow) <= this.userSpecifiedTimeInMillis;
	}
	
	@Override
	protected boolean lt(Tuple currentRow)
	{
		return this.getCurrentValue (currentRow) < this.userSpecifiedTimeInMillis;
	}
}
