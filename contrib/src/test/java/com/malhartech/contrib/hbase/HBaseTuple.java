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
  private String col1val;
  private String col2val;

  public String getCol1val()
  {
    return col1val;
  }

  public void setCol1val(String col1val)
  {
    this.col1val = col1val;
  }

  public String getCol2val()
  {
    return col2val;
  }

  public void setCol2val(String col2val)
  {
    this.col2val = col2val;
  }

}
