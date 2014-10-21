/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.goldengate.lib;

public class Employee
{
  int eid;
  String ename;
  int did;

  public String toString()
  {
    return "eid: " + eid +
           ", ename: " + ename +
           ", did: " + did;
  }
}
