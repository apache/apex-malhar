package com.datatorrent.demos.goldengate;

import com.datatorrent.api.LocalMode;
import org.junit.Test;

public class GoldenGateAppTest
{
  @Test
  public void testSomeMethod() throws Exception {
    LocalMode.runApp(new GoldenGateApp(), 30000);
  }
}
