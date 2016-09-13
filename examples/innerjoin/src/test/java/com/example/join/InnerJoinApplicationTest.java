package com.example.join;

import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.LocalMode;

public class InnerJoinApplicationTest
{
  @Test
  public void testApplication() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    lma.prepareDAG(new InnerJoinApplication(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    Thread.sleep(10 * 1000);
    lc.shutdown();
  }
}