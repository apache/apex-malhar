package com.datatorrent.contrib.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author prerna
 */
public class CouchBaseGetTest {

    public static List<Object> tuples;
    protected transient CouchbaseClient client;
    List<URI> baseURIs = new ArrayList<URI>();

    
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
    public void test() {
        
        URI uri = null;
        try {
             uri = new URI("http://node13.morado.com:8091/pools");

        } catch (URISyntaxException ex) {
          Logger.getLogger(CouchBaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        baseURIs.add(uri);
        CouchbaseClient client = null;
       
            try {
            CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
            cfb.setOpTimeout(10000);  // wait up to 10 seconds for an operation to succeed
            cfb.setOpQueueMaxBlockTime(5000); // wait up to 5 seconds when trying to enqueue an operation
            
            client = new CouchbaseClient(cfb.buildCouchbaseConnection(baseURIs,"default","default",""));
        } catch (IOException ex) {
            Logger.getLogger(CouchBaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        client.flush();
       long startTime = System.currentTimeMillis();
        System.out.println("start time before get is " + startTime);        

        for (int k = 0; k < 1000; k++) {
		System.out.println("k " + k);
        
        for (int i = 0; i < 100; i++) {
            
            String value = client.get("Key" + (k*100 + i)).toString();
            System.out.println("value is " + value);
        }
        }
        long stopTime = System.currentTimeMillis();
             
        //System.out.println("stop time after set is" + stopTime);
        //System.out.println("Threads after set are" + Thread.activeCount());
        //startTime = System.currentTimeMillis();
        //System.out.println("start time before get is " + startTime);
        //int count = futures.size();
        //System.out.println("count is " + count);
        
        /*for(int j=0;j<count;j++)
        {
            String value = client.get("Key" + j*2).toString();
            System.out.println("value is" + value);
        }*/
        
        /*while (!futures.isEmpty()) {
            Iterator<OperationFuture<Boolean>> iter = futures.iterator();
            while (iter.hasNext()) {
                OperationFuture<Boolean> future = iter.next();
                String key = future.getKey();
                System.out.println("key is" + key);
                String value = client.get(key).toString();
                System.out.println("value is" + value);
                if (future.isDone()) {
                    future.
                    iter.remove();
                }
            }
        }*/

        //stopTime = System.currentTimeMillis();
        System.out.println("stop time after get is " + stopTime);
        System.out.println("Threads after get are + " + Thread.activeCount());
        
        client.shutdown();

    }

}


