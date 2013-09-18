package com.datatorrent.demos.sitealerts;

import org.junit.Test;

import com.datatorrent.api.LocalMode;


public class ApacheAccessLogAnalaysisTest
{

  @Test
  public void testSomeMethod() throws Exception {
    LocalMode.runApp(new ApacheAccessLogAnalaysis(), 10000);
  }
}
