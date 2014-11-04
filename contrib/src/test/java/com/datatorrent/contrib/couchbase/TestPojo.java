/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.couchbase;

import java.util.HashMap;


public class TestPojo
{
  private String name;

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public HashMap<String, Integer> getMap()
  {
    return map;
  }

  public void setMap(HashMap<String, Integer> map)
  {
    this.map = map;
  }

  public Integer getPhone()
  {
    return phone;
  }

  public void setPhone(Integer phone)
  {
    this.phone = phone;
  }
  private HashMap<String,Integer> map;
  private Integer phone;


}
