package com.datatorrent.demos.goldengate.utils;

import java.io.Serializable;

import com.goldengate.atg.datasource.DsColumn;

public class _DsColumn implements Serializable
{
  /**
   * 
   */
  private static final long serialVersionUID = -5242559148477239397L;

  public String getBeforeValue()
  {
    return beforeValue;
  }

  public void setBeforeValue(String beforeValue)
  {
    this.beforeValue = beforeValue;
  }

  public String getAfterValue()
  {
    return afterValue;
  }

  public void setAfterValue(String afterValue)
  {
    this.afterValue = afterValue;
  }

  public boolean isChanged()
  {
    return changed;
  }

  public void setChanged(boolean changed)
  {
    this.changed = changed;
  }

  public boolean isMissing()
  {
    return missing;
  }

  public void setMissing(boolean missing)
  {
    this.missing = missing;
  }

  public String getValue()
  {
    return value;
  }

  public void setValue(String value)
  {
    this.value = value;
  }

  private String beforeValue = "";

  private String afterValue = "";

  private String value = "";
  
  private byte[] binary;

  private boolean changed;

  private boolean missing;

  public void readFromDsColumn(DsColumn dcol)
  {
    afterValue = dcol.getAfterValue();
    beforeValue = dcol.getBeforeValue();
    changed = dcol.isChanged();
    missing = dcol.isMissing();

    try {
      setValue(dcol.getValue());
    } catch (Exception e) {
      setValue("");
    }
    
    try {
      setBinary(dcol.getBinary());
    } catch (Exception e) {
      setBinary(null);
    }
  }

  @Override
  public String toString()
  {
    return "_DsColumn [beforeValue=" + beforeValue + ", afterValue=" + afterValue + ", value=" + value + ", changed=" + changed + ", missing=" + missing + "]";
  }

  public byte[] getBinary()
  {
    return binary;
  }

  public void setBinary(byte[] binary)
  {
    this.binary = binary;
  }

}
