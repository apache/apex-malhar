package com.datatorrent.contrib.common;

import javax.validation.constraints.NotNull;

@SuppressWarnings("rawtypes")
public class FieldInfo
{
  //Columns name set by user.
	@NotNull
  private String columnName;
	
  //Expressions set by user to get field values from input tuple.
  @NotNull
  private String columnExpression;

  private SupportType type;

  public FieldInfo()
  {
  }

  public FieldInfo(String columnName, String columnExpression, SupportType type)
  {
    setColumnName( columnName );
    setColumnExpression( columnExpression );
    setType( type );
  }
  
  /**
   * the column name which keep this field.
   */
	public String getColumnName()
	{
		return columnName;
	}

	public void setColumnName(String columnName)
	{
		this.columnName = columnName;
	}

	/**
	 * Java expressions that will generate the column value from the POJO.
	 * 
	 */
	public String getColumnExpression()
	{
		return columnExpression;
	}

	/**
	 * Java expressions that will generate the column value from the POJO.
	 * 
	 */
	public void setColumnExpression(String expression)
	{
		this.columnExpression = expression;
	}

	/**
	 * the columnName should not duplicate( case-insensitive )
	 */
	@Override
	public int hashCode()
	{
	  return columnName.toLowerCase().hashCode();
	}
	
	@Override
	public boolean equals( Object obj )
	{
	  if( obj == null || !( obj instanceof FieldInfo ) )
	    return false;
	  return columnName.equalsIgnoreCase( ((FieldInfo)obj).getColumnName() );
	}
	
	/**
	 * the Java type of the column
	 */
	public SupportType getType()
	{
		return type;
	}

	/**
	 * the Java type of the column
	 */
	public void setType( SupportType type )
	{
		this.type = type;
	}
  
	
	public static enum SupportType
	{
	  BOOLEAN( Boolean.class ), 
	  SHORT( Short.class ), 
	  INTEGER( Integer.class ), 
	  LONG( Long.class ), 
	  FLOAT( Float.class ), 
	  DOUBLE( Double.class ), 
	  STRING( String.class );
	  
	  private Class javaType;
	  private SupportType( Class javaType )
	  {
	    this.javaType = javaType;
	  }
	  
	  public Class getJavaType()
	  {
	    return javaType;
	  }
	}
  
  
}
