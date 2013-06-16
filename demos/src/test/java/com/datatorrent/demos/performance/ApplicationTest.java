/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.performance;

import java.io.IOException;

import org.junit.Test;

import com.datatorrent.api.LocalMode;
import com.datatorrent.demos.performance.Application;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{
  @Test
  public void testApplication() throws IOException, Exception
  {
    LocalMode.runApp(new Application(), 60000);
  }
}
