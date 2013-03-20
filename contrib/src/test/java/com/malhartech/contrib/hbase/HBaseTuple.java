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
  private String col1Value;
  private String col2Value;

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

  public String getCol1Value()
  {
    return col1Value;
  }

  public void setCol1Value(String col1Value)
  {
    this.col1Value = col1Value;
  }

  public String getCol2Value()
  {
    return col2Value;
  }

  public void setCol2Value(String col2Value)
  {
    this.col2Value = col2Value;
  }

}
