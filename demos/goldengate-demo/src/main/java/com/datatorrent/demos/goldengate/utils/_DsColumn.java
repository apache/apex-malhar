/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.goldengate.utils;

import java.io.Serializable;
import com.goldengate.atg.datasource.DsColumn;

/**
 * A serializable version of Golden Gate's DsColumn object.
 */
public class _DsColumn implements Serializable
{
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

  /**
   * Loads the data from the given DsColumn object.
   * @param dt The DsColumn object to load data from.
   */
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
