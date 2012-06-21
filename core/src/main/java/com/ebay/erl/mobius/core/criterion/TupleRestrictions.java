package com.ebay.erl.mobius.core.criterion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.collection.CaseInsensitiveTreeSet;
import com.ebay.erl.mobius.core.model.Tuple;

/**
 * Factory class that provides methods to define {@link TupleCriterion}
 * for filtering {@link Tuple}s in a {@link Dataset}.
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
 */
public class TupleRestrictions
{	
	/**
	 * Hadoop configuration
	 */
	protected static Configuration conf;
	
	
	/**
	 * Setup Hadoop configuration.
	 */
	public static final void configure(Configuration conf)
	{
		TupleRestrictions.conf = conf;
	}
	
	private static File checkFileExist(File file)
		throws FileNotFoundException
	{
		if( TupleRestrictions.conf!=null && TupleRestrictions.conf.get ("mobius.studio.workspace.base")!=null )
		{
			File base = new File(TupleRestrictions.conf.get ("mobius.studio.workspace.base"));
			File f = new File(base, file.getName ());
			if( !f.exists () )
			{
				throw new FileNotFoundException("File not found in:"+f.getAbsolutePath ());
			}
			return f;
		}
		else
		{		
			if( !file.exists () )
			{
				throw new FileNotFoundException("File not found:"+file.getAbsolutePath ());
			}
			return file;
		}
	}
	
	
	/**
	 * Create a {@link TupleCriterion} that only accept tuples with 
	 * the value of the specified <code>column</code> that is within
	 * the provide <code>list</code>. 
	 * <p>
	 * 
	 * The value of the <code>column</code> will be converted into
	 * string, if it's not string, to compare.
	 */
	public static TupleCriterion withinString(final String column, final ArrayList<String> list)
	{
		return new StringCriterion(column, list, RelationalOperator.WITHIN);
	}
	
	
	/**
	 * Create a {@link TupleCriterion} that only accepts tuples with 
	 * the value of the specified <code>column</code> that is within
	 * the provide <code>list</code>.
	 * <p>
	 * 
	 * The value of the <code>column</code> will be converted into
	 * double, if it's not number, to compare.
	 */
	public static TupleCriterion withinNumber(final String column, final ArrayList<Double> list)
	{
		return new NumberCriterion(column, list, RelationalOperator.WITHIN);
	}
	
	
	
	/**
	 * Create a tuple criterion that only accepts tuples when the value 
	 * of the <code>column</code> are presented in the given <code>file</code>
	 * <p>
	 * 
	 * The assumption of the file is that, it's single column and one to many
	 * line text file.  Each line is read into a case insensitive set, and 
	 * using the set to check the value of the <code>column</code> within
	 * the set or not.
	 * 
	 * 
	 * @param column the name of a column to be tested that whether its value is in 
	 * the given <code>file</code> or not
	 * 
	 * @param file a single column and multiple lines of file that contains strings/numbers,
	 * each line is treated as a single unit.
	 *
	 * @return an instance of {@link TupleCriterion} that extracts only the records 
	 * when the value of its <code>column</code> are presented in the given 
	 * <code>file</code>.
	 * 
	 * @throws FileNotFoundException if the given file cannot be found.
	 */
	public static TupleCriterion within(final String column, File file)
		throws FileNotFoundException
	{
		final File f = TupleRestrictions.checkFileExist (file);
		
		return new TupleCriterion(){			
			
			private static final long serialVersionUID = -1121221619118915652L;
			private Set<String> set;
			
			@Override
			public void setConf(Configuration conf)
			{
				try
				{
					if( conf.get ("tmpfiles")==null || conf.get ("tmpfiles").trim ().length ()==0 )
					{
						conf.set ("tmpfiles", validateFiles (f.getAbsolutePath (), conf));
					}
					else
					{
						conf.set ("tmpfiles", validateFiles (f.getAbsolutePath (), conf)+","+conf.get("tmpfiles"));
					}
					
				}
				catch ( IOException e )
				{
					throw new IllegalArgumentException(e);
				}
			}
			
			/**
			 * COPIED FROM org.apache.hadoop.util.GenericOptionsParser
			 */
			private String validateFiles(String files, Configuration conf) throws IOException
			{
				if ( files == null )
					return null;
				String[] fileArr = files.split (",");
				String[] finalArr = new String[fileArr.length];
				for ( int i = 0; i < fileArr.length; i++ )
				{
					String tmp = fileArr[i];
					String finalPath;
					Path path = new Path (tmp);
					URI pathURI = path.toUri ();
					FileSystem localFs = FileSystem.getLocal (conf);
					if ( pathURI.getScheme () == null )
					{
						// default to the local file system
						// check if the file exists or not first
						if ( !localFs.exists (path) )
						{
							throw new FileNotFoundException ("File " + tmp + " does not exist.");
						}
						finalPath = path.makeQualified (localFs).toString ();
					} else
					{
						// check if the file exists in this file system
						// we need to recreate this filesystem object to copy
						// these files to the file system jobtracker is running
						// on.
						FileSystem fs = path.getFileSystem (conf);
						if ( !fs.exists (path) )
						{
							throw new FileNotFoundException ("File " + tmp + " does not exist.");
						}
						finalPath = path.makeQualified (fs).toString ();
						try
						{
							fs.close ();
						} catch ( IOException e )
						{
						}
						;
					}
					finalArr[i] = finalPath;
				}
				return StringUtils.arrayToString (finalArr);
			}
			
			
				
			@Override
			protected boolean evaluate(Tuple tuple, Configuration configuration)
			{	
				if( set==null )
				{
					set = new CaseInsensitiveTreeSet();
					BufferedReader br = null;
					try
					{
						br = new BufferedReader(new FileReader(new File(f.getName ())));
						String newLine = null;
						while( (newLine=br.readLine ())!=null )
						{
							this.set.add (newLine);
						}
					}catch(IOException e)
					{
						throw new RuntimeException(e);
					}
					finally
					{
						try{br.close ();}catch(Throwable e){}
					}
				}
				
				String value = tuple.getString (column);
				if( value!=null )
				{
					return this.set.contains (value);
				}
				else
				{
					return false;
				}
			}
	
			@Override
			public String[] getInvolvedColumns()
			{				
				return new String[]{column};
			}
		};
	}
	
	
	
	/**
	 * Create a tuple criterion that only accepts tuples 
	 * with the value of <code>column</code> that is <b>NOT</b> 
	 * presented in the given <code>file</code>
	 * 
	 * The assumption of the file is that, it's single column and one to many
	 * line text file.  Each line is read into a case insensitive set, and 
	 * using the set to check the value of the <code>column</code> within
	 * the set or not.
	 * 
	 * @param column the name of a column to be tested that whether its value is in 
	 * the given <code>file</code> or not
	 * 
	 * @param file a single column and multiple lines of file that contains strings/numbers,
	 * each line is treated as a single unit.
	 * 
	 * @return an instance of {@link TupleCriterion} that extracts only the records 
	 * when the value of its <code>column</code> are <b>NOT</b>presented in the given 
	 * <code>file</code>.
	 * 
	 * @throws FileNotFoundException if the given file cannot be found.
	 */
	public static TupleCriterion not_within(final String column, final File file)
		throws FileNotFoundException
	{
		TupleCriterion criterion = TupleRestrictions.within (column, file);
		TupleCriterion notCriterion = criterion.not();
		return notCriterion;
	}
	
	
	/**
	 * Create a {@link TupleCriterion} that only accept tuples with 
	 * the value of the specified <code>column</code> is <b>not</b>
	 * within the provide <code>list</code>.
	 * <p>
	 * 
	 * The value of the <code>column</code> will be converted into
	 * double to compare, if it's not double.
	 */
	public static TupleCriterion notWithinNumber(final String column, final ArrayList<Double> values)		
	{		
		return TupleRestrictions.withinNumber(column, values).not();
	}
	
	
	/**
	 * Create a {@link TupleCriterion} that only accept tuples with 
	 * the value of the specified <code>column</code> is <b>not</b>
	 * within the provide <code>list</code>.
	 * <p>
	 * 
	 * The value of the <code>column</code> will be converted into
	 * string to compare, if it's not string.
	 */
	public static TupleCriterion notWithinString(final String column, final ArrayList<String> values)
	{		
		return TupleRestrictions.withinString(column, values).not();
	}
	
	
	/**
	 * Define a {@link TupleCriterion} that only extracts records when the value of the
	 * <code>column</code> meets the <cdoe>regex</code>.
	 * 
	 * @param column the name of a column to be tested on its value whether it meets
	 * the specified <code>regex</code> or not.
	 * 
	 * @param regex a regular expression to test.
	 * 
	 * @return a {@link TupleCriterion} accepts value from the <code>column</code>
	 * match the given <code>regex</code>.
	 * 
	 */	
	public static TupleCriterion regex(final String column, final String regex)
	{
		return new TupleCriterion(){			
			
			private static final long serialVersionUID = -6630104271777176036L;
			private transient Pattern pattern = Pattern.compile (regex);
			private transient Matcher matcher = pattern.matcher ("");
			
			@Override
			protected boolean evaluate(Tuple tuple, Configuration configuration)
			{	
				if( pattern==null )
				{
					pattern = Pattern.compile (regex);
					matcher = pattern.matcher ("");
				}
				
				String value = tuple.getString (column);
				if( value!=null )
				{
					matcher.reset (value);
					return matcher.find ();
				}
				else
				{
					return false;
				}
			}

			@Override
			public String[] getInvolvedColumns()
			{				
				return new String[]{column};
			}};
	}
	
	
	/**
	 * Create a {@link TupleCriterion} that only accepts 
	 * tuples with the value of the given <code>column<code>
	 * is not null nor empty string.
	 */
	public static TupleCriterion notNull(final String column)
	{
		
		return new TupleCriterion(){

			private static final long serialVersionUID = 1573625916312469904L;

			@Override
			protected boolean evaluate(Tuple tuple, Configuration configuration)
			{				
				return tuple.get (column)!=null && tuple.getString (column).trim ().length ()>0;
			}

			@Override
			public String[] getInvolvedColumns()
			{
				return new String[]{column};
			}};
	}
	
	
	
	/**
	 * Specify the given <code>column</code>'s value equals to <code>value</code>
	 */
	public static TupleCriterion eq(String column, String value)
	{
		return new StringCriterion(column, value, RelationalOperator.EQ);
	}
	
	
	
	/**
	 * Specify the given <code>column</code>'s value equals to <code>value</code>
	 */
	public static TupleCriterion eq(String column, Number value)
	{
		return new NumberCriterion(column, value.doubleValue(), RelationalOperator.EQ);
	}
	
	
	
	/**
	 * Specify the given <code>column</code>'s value equals to <code>trueFalse</code>
	 */
	public static TupleCriterion eq(final String column, final boolean trueFalse)
	{
		return new TupleCriterion()
		{			
			private static final long serialVersionUID = 3652448730224390852L;

			@Override
			protected boolean evaluate(Tuple tuple, Configuration configuration)
			{				
				return tuple.getBoolean(column)==trueFalse;
			}

			@Override
			public String[] getInvolvedColumns() 
			{				
				return new String[]{column};
			}
			
		};
	}
	
	
	
	/**
	 * Return a {@link TupleCriterion} that parses the value of <column>column</column>
	 * with the given <column>columnDateFormat</column> into milliseconds, comparing the
	 * milliseconds (A) with the <code>date</code> (B) and only accept tuples records when 
	 * A equals to B.
	 * 
	 * @param column name of a column to be tested in a dataset.
	 * 
	 * @param columnDateFormat the date format of the specified <code>column</code> in the dataset. 
	 * The <code>columnFormat</code> pattern is the same as {@link java.text.SimpleDateFormat}
	 * 
	 * @param date a date constraint to be test.
	 * 
	 */
	public static TupleCriterion eq(String column, String columnDateFormat, java.util.Date date)
	{
		return new DateCriterion(column, columnDateFormat, date.getTime(), RelationalOperator.EQ);
	}
	
	
	/**
	 * Return a {@link TupleCriterion} that only accepts tuples with 
	 * the value of <code>column</code> is equal to the specified 
	 * <code>date</code>.
	 * <p>
	 * 
	 * If the type of the value for the <code>column</code> is and instance 
	 * of {@link java.util.Date}, then the comparison is done by calling the
	 * method of {@link java.util.Date#getTime()} for the value and compare
	 * it with <code>date.getTime()</code>.
	 * <p>
	 * 
	 * If the type of the value is not an instance of {@link java.util.Date},
	 * then it will be parsed into date format using either the format of 
	 * <code>yyyy-MM-dd</code> or <code>yyyy-MM-dd HH:mm:ss</code>.
	 * 
	 */
	public static TupleCriterion eq(String column, java.util.Date date)
	{
		return new DateCriterion(column, null, date.getTime(), RelationalOperator.EQ);
	}
	
	
	
	/**
	 * Create a {@link TupleCriterion} that only accepts tuples with 
	 * the two columns' values are equals.
	 */
	public static TupleCriterion eqColumns(final String column1, final String column2)
	{
		return new ColumnsCriterion(column1, column2, RelationalOperator.EQ);
	}
	
	
	
	
	
	/**
	 * not equals
	 */
	public static TupleCriterion ne(String columnName, String value)
	{
		return new StringCriterion(columnName, value, RelationalOperator.NE);
	}	
	public static TupleCriterion ne(String columnName, Number value)
	{
		return new NumberCriterion(columnName, value.doubleValue(), RelationalOperator.NE);
	}
	public static TupleCriterion ne(String columnName, String columnFormat, Date date)
	{
		return new DateCriterion(columnName, columnFormat, date.getTime (), RelationalOperator.NE);
	}
	public static TupleCriterion ne(String columnName, Date date)
	{
		return new DateCriterion(columnName, null, date.getTime (), RelationalOperator.NE);
	}
	public static TupleCriterion ne(String columnName, String columnFormat, Calendar date)
	{
		return new DateCriterion(columnName, columnFormat, date.getTimeInMillis (), RelationalOperator.NE);
	}
	public static TupleCriterion ne(String columnName, Calendar date)
	{
		return new DateCriterion(columnName, null, date.getTimeInMillis (), RelationalOperator.NE);
	}
	public static TupleCriterion ne(final String column, final boolean trueFalse)
	{
		return new TupleCriterion()
		{			
			private static final long serialVersionUID = 3652448730224390852L;

			@Override
			protected boolean evaluate(Tuple tuple, Configuration configuration)
			{				
				return tuple.getBoolean(column)!=trueFalse;
			}

			@Override
			public String[] getInvolvedColumns() 
			{				
				return new String[]{column};
			}
			
		};
	}
	/**
	 * compare if two column's values are not equals.
	 */
	public static TupleCriterion neColumns(final String column1, final String column2)
	{
		return new ColumnsCriterion(column1, column2, RelationalOperator.NE);
	}
	
	
	
	/**
	 * greater than
	 */
	public static TupleCriterion gt(String columnName, String value)
	{
		return new StringCriterion(columnName, value, RelationalOperator.GT);
	}
	public static TupleCriterion gt(String columnName, Number value)
	{
		return new NumberCriterion(columnName, value.doubleValue(), RelationalOperator.GT);
	}	
	public static TupleCriterion gt(String columnName, String columnFormat, Date date)
	{
		return new DateCriterion(columnName, columnFormat, date.getTime (), RelationalOperator.GT);
	}
	public static TupleCriterion gt(String columnName, Date date)
	{
		return new DateCriterion(columnName, null, date.getTime (), RelationalOperator.GT);
	}
	public static TupleCriterion gt(String columnName, String columnFormat, Calendar date)
	{
		return new DateCriterion(columnName, columnFormat, date.getTimeInMillis (), RelationalOperator.GT);
	}
	public static TupleCriterion gt(String columnName, Calendar date)
	{
		return new DateCriterion(columnName, null, date.getTimeInMillis (), RelationalOperator.GT);
	}
	/**
	 * compare if column1's value greater than column2's value
	 */
	public static TupleCriterion gtColumns(final String column1, final String column2)
	{
		return new ColumnsCriterion(column1, column2, RelationalOperator.GT);
	}
	
	
	
	/**
	 * greater than or equal
	 */
	public static TupleCriterion ge(String columnName, String value)
	{
		return new StringCriterion(columnName, value, RelationalOperator.GE);
	}	
	public static TupleCriterion ge(String columnName, Number value)
	{
		return new NumberCriterion(columnName, value.doubleValue(), RelationalOperator.GE);
	}
	public static TupleCriterion ge(String columnName, String columnFormat, Date date)
	{
		return new DateCriterion(columnName, columnFormat, date.getTime (), RelationalOperator.GE);
	}
	public static TupleCriterion ge(String columnName, Date date)
	{
		return new DateCriterion(columnName, null, date.getTime (), RelationalOperator.GE);
	}
	public static TupleCriterion ge(String columnName, String columnFormat, Calendar date)
	{
		return new DateCriterion(columnName, columnFormat, date.getTimeInMillis (), RelationalOperator.GE);
	}
	public static TupleCriterion ge(String columnName, Calendar date)
	{
		return new DateCriterion(columnName, null, date.getTimeInMillis (), RelationalOperator.GE);
	}
	/**
	 * compare if column1's value greater or equals to column2's value
	 */
	public static TupleCriterion geColumns(final String column1, final String column2)
	{
		return new ColumnsCriterion(column1, column2, RelationalOperator.GE);
	}
	
	
	
	/**
	 * less than or equal
	 */
	public static TupleCriterion le(String columnName, String value)
	{
		return new StringCriterion(columnName, value, RelationalOperator.LE);
	}
	public static TupleCriterion le(String columnName, Number value)
	{
		return new NumberCriterion(columnName, value.doubleValue(), RelationalOperator.LE);
	}
	public static TupleCriterion le(String columnName, String columnFormat, Date date)
	{
		return new DateCriterion(columnName, columnFormat, date.getTime (), RelationalOperator.LE);
	}
	public static TupleCriterion le(String columnName, Date date)
	{
		return new DateCriterion(columnName, null, date.getTime (), RelationalOperator.LE);
	}
	public static TupleCriterion le(String columnName, String columnFormat, Calendar date)
	{
		return new DateCriterion(columnName, columnFormat, date.getTimeInMillis (), RelationalOperator.LE);
	}
	public static TupleCriterion le(String columnName, Calendar date)
	{
		return new DateCriterion(columnName, null, date.getTimeInMillis (), RelationalOperator.LE);
	}
	/**
	 * compare if column1's value less than column2's value
	 */
	public static TupleCriterion leColumns(final String column1, final String column2)
	{
		return new ColumnsCriterion(column1, column2, RelationalOperator.LE);
	}
	
	
	
	/**
	 * less than
	 */
	public static TupleCriterion lt(String columnName, String value)
	{
		return new StringCriterion(columnName, value, RelationalOperator.LT);
	}
	public static TupleCriterion lt(String columnName, Number value)
	{
		return new NumberCriterion(columnName, value.doubleValue(), RelationalOperator.LT);
	}
	public static TupleCriterion lt(String columnName, String columnFormat, Date date)
	{
		return new DateCriterion(columnName, columnFormat, date.getTime (), RelationalOperator.LT);
	}
	public static TupleCriterion lt(String columnName, Date date)
	{
		return new DateCriterion(columnName, null, date.getTime (), RelationalOperator.LT);
	}
	public static TupleCriterion lt(String columnName, String columnFormat, Calendar date)
	{
		return new DateCriterion(columnName, columnFormat, date.getTimeInMillis (), RelationalOperator.LT);
	}
	public static TupleCriterion lt(String columnName, Calendar date)
	{
		return new DateCriterion(columnName, null, date.getTimeInMillis (), RelationalOperator.LT);
	}
	/**
	 * compare if column1's value less or equals to column2's value
	 */
	public static TupleCriterion ltColumns(final String column1, final String column2)
	{
		return new ColumnsCriterion(column1, column2, RelationalOperator.LT);
	}
}
