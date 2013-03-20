/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import java.io.Serializable;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class HBaseTuple implements Serializable
{
  private String row;
  private String colFamily;
  private String colName;
  private String colValue;

  public String getRow()
  {
    return row;
  }

  public void setRow(String row)
  {
    this.row = row;
  }

  public String getColFamily()
  {
    return colFamily;
  }

  public void setColFamily(String colFamily)
  {
    this.colFamily = colFamily;
  }

  public String getColName()
  {
    return colName;
  }

  public void setColName(String colName)
  {
    this.colName = colName;
  }

  public String getColValue()
  {
    return colValue;
  }

  public void setColValue(String colValue)
  {
    this.colValue = colValue;
  }

}
