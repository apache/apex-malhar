package com.datatorrent.contrib.goldengate;

import com.datatorrent.api.LocalMode;
import com.datatorrent.contrib.goldengate.app.GoldenGateApp;
import org.junit.Test;

public class GoldenGateAppTest
{
  @Test
  public void testSomeMethod() throws Exception {
    LocalMode.runApp(new GoldenGateApp(), 30000);
  }
}
