package com.datatorrent.demos.sitealerts;

import org.junit.Test;

import com.datatorrent.api.LocalMode;


public class ApacheAccessLogAnalysisTest
{

  @Test
  public void testSomeMethod() throws Exception {
    LocalMode.runApp(new ApacheAccessLogAnalysis(), 10000);
  }
}
