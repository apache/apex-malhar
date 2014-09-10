
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
    LocalMode.runApp(new CouchBaseAppOutput(), 10000);
  }
}
