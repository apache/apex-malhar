/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.mobile;

import org.junit.Test;

import com.malhartech.api.LocalMode;


public class ApplicationAlertTest
{
  public ApplicationAlertTest()
  {
  }

  /**
   * Test of getApplication method, of class Application.
   */
  @Test
  public void testGetApplication() throws Exception
  {
    LocalMode.runApp(new ApplicationAlert(), 60000);
  }
}
