
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
    CouchBaseStore store = new CouchBaseStore();
        try {
            store.addNodes("http://node26.morado.com:8091/pools");
            store.connect();
        } catch (IOException ex) {
            Logger.getLogger(CouchBaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }
}
 
        
}

