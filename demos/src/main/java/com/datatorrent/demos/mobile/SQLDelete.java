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
public class SQLDelete extends SQLQuery
{

  private String name;
  private List<String> values;

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public List<String> getValues()
  {
    return values;
  }

  public void setValues(List<String> values)
  {
    this.values = values;
  }


}
