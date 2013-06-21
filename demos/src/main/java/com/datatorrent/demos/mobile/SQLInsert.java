/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.mobile;

import java.util.List;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class SQLInsert extends SQLQuery
{

  private List<String> names;
  private List<List<String>> values;

  public List<String> getNames()
  {
    return names;
  }

  public void setNames(List<String> names)
  {
    this.names = names;
  }

  public List<List<String>> getValues()
  {
    return values;
  }

  public void setValues(List<List<String>> values)
  {
    this.values = values;
  }


}
