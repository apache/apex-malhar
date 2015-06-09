package com.datatorrent.contrib.common;

import java.util.List;

import javax.validation.constraints.NotNull;

public class TableInfo< T extends FieldInfo >
{
  //the row or id expression
  private String rowOrIdExpression;

  //this class should be used in configuration which don't support Generic.
  @NotNull
  private List<T> fieldsInfo;

  /**
   * expression for Row or Id
   */
  public String getRowOrIdExpression()
  {
    return rowOrIdExpression;
  }

  /**
   * expression for Row or Id
   */
  public void setRowOrIdExpression(String rowOrIdExpression)
  {
    this.rowOrIdExpression = rowOrIdExpression;
  }

	/**
	 * the field information. each field of the tuple related to on field info.
	 */
	public List<T> getFieldsInfo()
	{
		return fieldsInfo;
	}

	/**
	 * the field information. each field of the tuple related to on field info.
	 */
	public void setFieldsInfo(List<T> fieldsInfo)
	{
		this.fieldsInfo = fieldsInfo;
	}
  
  
}
