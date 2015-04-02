package com.ebay.erl.mobius.core.model;

import com.ebay.erl.mobius.core.builder.Dataset;
import com.ebay.erl.mobius.core.function.base.ExtendFunction;
import com.ebay.erl.mobius.core.function.base.Projectable;

/**
 * Represents a single column of a {@link Dataset}. An instance of
 * {@link Column} can be used as projection in the <code>list</code>,
 * <code>join</code>, <code>grouping</code>, or <code>sorting</code>
 * jobs.
 * 
 * <p>
 * This product is licensed under the Apache License,  Version 2.0, 
 * available at http://www.apache.org/licenses/LICENSE-2.0.
 * 
 * This product contains portions derived from Apache hadoop which is 
 * licensed under the Apache License, Version 2.0, available at 
 * http://hadoop.apache.org.
 * 
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Jack Shen, Gyanit Singh, Neel Sundaresan
 */
public class Column extends ExtendFunction
{
	private static final long serialVersionUID = -4969514776026947458L;
	
	private final int hashCode;
	
	/**
	 * the {@link Dataset} this column belongs to.
	 */
	protected Dataset dataset;
	
	
	
	/**
	 * the name of this column
	 */
	protected String columnName;
	
	
	
	/**
	 * the new name of this column, specified by user via
	 * {@link #setNewName(String)}
	 */
	protected String newName;
	
	
	
	/**
	 * Create a column instance that represents a column
	 * named <code>columnName</code> in the <code>dataset
	 * </code>.
	 * 
	 * @param dataset a {@linkplain Dataset} contains the specified column.
	 * @param columnName name of the column
	 * @throws IllegalArgumentException if the specified <code>columnName</code>
	 * doesn't exist in the <code>dataset</code>.
	 */
	public Column(Dataset dataset, String columnName)
	{	
		this.dataset	= dataset;
		this.columnName = columnName;
		this.newName	= this.columnName;
		if ( !this.dataset.withinSchema(columnName) )
		{
			throw new IllegalArgumentException(columnName+" doesn't define in dataset:"+this.dataset);
		}
		
		this.hashCode = this.dataset.hashCode() + this.columnName.hashCode();
		
		this.init(new Column[]{this});
	}
	
	
	
	/**
	 * @deprecated use {@linkplain #setNewName(String)} instead.
	 */ 
	@Override
	public final Projectable setOutputSchema(String... schema)
	{
		if( schema.length!=1 )
			throw new IllegalArgumentException("Column can only have one single column in the schema.");
		
		this.setNewName(schema[0]);
		return this;
	}
	
	
	
	/**
	 * Always return a size 1 array which contains the result 
	 * of {@link #getOutputName()}.
	 */
	@Override
	public final String[] getOutputSchema()
	{
		return new String[]{this.getOutputName()};
	}
	
	
	/**
	 * Change the output schema name of this column.
	 * <p>
	 * 
	 * By default, the output schema of this column
	 * is the name specified in the constructor, in
	 * the case of select two different columns from
	 * two different datasets, and the name of these
	 * two columns are the same, one can use this
	 * method to change the name of one of the columns
	 * to remove ambiguous.
	 * 
	 * 
	 * @return the column it self.
	 */
	public final Column setNewName(String newName)
	{
		this.newName = newName;
		return this;
	}
	
	/**
	 * Get the {@link Dataset} this column belongs to.
	 */
	public final Dataset getDataset()
	{
		return this.dataset;
	}
	
	@Override
	public String toString()
	{
		return "Column(input column name:"+this.columnName+", output name:"+this.newName+") in Dataset:"+this.dataset.toString();
	}
	
	@Override
	public int hashCode()
	{
		return this.hashCode;
	}
	
	/**
	 * Two columns are equal only if their <code>dataset</code>
	 * are the same (specified by {@linkplain Dataset#equals(Object)}),
	 * <code>columnName</code> are the same, and their {@link #getOutputName()}
	 * also the same.
	 */
	@Override
	public boolean equals(Object obj)
	{
		if( obj instanceof Column)
		{
			if(obj==this)
				return true;
			
			Column that = (Column)obj;
			
			if ( this.dataset.equals(that.dataset) )
			{
				if( this.columnName.equals(that.columnName) )
				{
					if( this.newName.equals(that.newName) )
					{
						return true;
					}
				}
			}
		}
		return false;
	}
	
	/**
	 * Select multiple <code>columns</code> from a <code>dataset</code>
	 */
	public static Column[] columns(Dataset dataset, String... columns)
	{
		if( columns.length==0 )
			throw new IllegalArgumentException("Please give at least one column name.");
		
		Column[] result = new Column[columns.length];
		for( int i=0;i<columns.length;i++ )
		{
			result[i] = new Column(dataset, columns[i]);
		}
		return result;
	}
	
	/**
	 * Return the original name of this column
	 */
	public final String getInputColumnName()
	{
		return this.columnName;
	}
	
	
	/**
	 * Get the output name of this column, by default,
	 * it's the same as it's original name.
	 */
	public final String getOutputName()
	{
		return this.newName;
	}
	
	
	/**
	 * Concatenate the {@link #getOutputName()} from the 
	 * <code>columns</code> together and separated by comma.
	 */
	public static String toSchemaString(Column... columns)
	{
		if( columns==null || columns.length==0)
			throw new IllegalArgumentException("Please specify at least one column object.");
		
		StringBuffer schema = new StringBuffer();
		for( int i=0;i<columns.length;i++ )
		{
			schema.append(columns[i].getOutputName());
			if( i<columns.length-1 )
				schema.append(",");
		}
		return schema.toString();
	}
	
	
	/**
	 * Get the {@link #getOutputName()} from the <code>columns</code>
	 * and store then in the returned string array.
	 */
	public static String[] toSchemaArray(Column... columns)
	{
		if( columns==null || columns.length==0)
			throw new IllegalArgumentException("Please specify at least one column object.");
		
		String[] schema = new String[columns.length];
		for( int i=0;i<columns.length;i++ )
		{
			schema[i] = columns[i].getInputColumnName();
		}
		return schema;
	}


	/**
	 * Return a {@link Tuple} which contains one single column with the
	 * name of {@link #getOutputName()} and its value is from the column 
	 * named {@link #getInputColumnName()} in the <code>inputRow</code>.
	 */
	@Override
	public Tuple getResult(Tuple inputRow) 
	{
		Tuple result = new Tuple();
		result.insert(this.getOutputName(), inputRow.get(this.getInputColumnName()));
		return result;
	}
}
