/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.io.fs;

import org.junit.rules.TestWatcher;

public class IOTestHelper extends TestWatcher
{
  public String dir = null;

  @Override
  protected void starting(org.junit.runner.Description description)
  {
    String methodName = description.getMethodName();
    String className = description.getClassName();
    this.dir = "target/" + className + "/" + methodName;
  }

}
