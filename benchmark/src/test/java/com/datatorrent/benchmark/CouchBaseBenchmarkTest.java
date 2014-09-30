package com.datatorrent.benchmark;

import com.datatorrent.api.LocalMode;
import org.junit.Test;

/**
 *
 * @author prerna
 */
public class CouchBaseBenchmarkTest {

    @Test
    public void testCouchBaseAppOutput() throws Exception {
        LocalMode.runApp(new CouchBaseAppOutput(), 300000);
    }

    @Test
    public void testCouchBaseAppInput() throws Exception {
        LocalMode.runApp(new CouchBaseAppInput(), 30000);
    }

    @Test
    public void testCouchBaseAppUpdate() throws Exception {
        LocalMode.runApp(new CouchBaseAppUpdate(), 30000);
    }
}
