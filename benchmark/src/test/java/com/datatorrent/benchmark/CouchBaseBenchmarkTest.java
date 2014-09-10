
package com.datatorrent.benchmark;

import com.datatorrent.api.LocalMode;
import org.junit.Test;

/**
 *
 * @author prerna
 */


public class CouchBaseBenchmarkTest {
    @Test
  public void testSomeMethod() throws Exception {
    try{
    LocalMode.runApp(new CouchBaseApp(), 30000);
    }
    catch(RuntimeException e)
   {
      e.getCause().printStackTrace();
   }
  }
}
