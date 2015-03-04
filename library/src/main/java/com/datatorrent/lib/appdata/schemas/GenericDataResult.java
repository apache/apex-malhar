/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.qr.Result;

import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericDataResult extends Result
{
  private List<String> selectedFields;

  public GenericDataResult()
  {
  }

  /**
   * @return the selectedFields
   */
  public List<String> getSelectedFields()
  {
    return selectedFields;
  }

  /**
   * @param selectedFields the selectedFields to set
   */
  public void setSelectedFields(List<String> selectedFields)
  {
    this.selectedFields = selectedFields;
  }
}
