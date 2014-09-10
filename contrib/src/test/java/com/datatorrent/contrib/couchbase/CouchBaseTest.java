
package com.datatorrent.contrib.couchbase;

import com.datatorrent.api.BaseOperator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 *
 * @author prerna
 */


public class CouchBaseTest extends BaseOperator{
    public static List<Object> tuples;
 
    public CouchBaseTest() {
        
        tuples = new ArrayList<Object>();
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
        
    }
    
    @After
    public void tearDown() {
    }

  
@Test
public void test()
{
    CouchBaseWindowStore store = new CouchBaseWindowStore();
        try {
            store.addNodes("node26.morado.com");
            store.connect();
            store.getInstance().set("prer", 12345);
            Integer output = (Integer) store.getInstance().get("prer");
            Logger.getLogger(CouchBaseTest.class.getName()).log(Level.SEVERE,output.toString());
        } catch (IOException ex) {
            Logger.getLogger(CouchBaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }
} 
        
}

