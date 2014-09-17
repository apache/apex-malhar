package com.datatorrent.contrib.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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
public class CouchBaseTest {

    public static List<Object> tuples;
    protected transient CouchbaseClient client;
    List<URI> baseURIs = new ArrayList<URI>();

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
    public void test() {
        List<String> output = new ArrayList<String>();
        List<OperationFuture<Boolean>> futures = new ArrayList<OperationFuture<Boolean>>();
        URI uri = null;
        try {
            uri = new URI("http://node26.morado.com:8091/pools");
        } catch (URISyntaxException ex) {
            Logger.getLogger(CouchBaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        baseURIs.add(uri);
        try {
            client = new CouchbaseClient(new CouchbaseConnectionFactoryBuilder()
                      .setViewTimeout(300000) // set the timeout to 30 seconds
                      .setViewWorkerSize(5) // use 5 worker threads instead of one
                      .setViewConnsPerNode(20) // allow 20 parallel http connections per node in the cluster
                    .buildCouchbaseConnection(baseURIs, "default", ""));
        } catch (IOException ex) {
            Logger.getLogger(CouchBaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        client.flush();

        final AtomicInteger j = new AtomicInteger();
        long startTime = System.currentTimeMillis();
        System.out.println("start time before set is " + startTime);
	for (int k = 0; k < 10000; ++k) {
		System.out.println("k " + k);
        final CountDownLatch countLatch = new CountDownLatch(5);
        for (int i = 0; i < 5; i++) {
            
            final OperationFuture<Boolean> future = client.set("Key" + (k*5 + i), i);
		System.out.println("i " + i);
            /* try {
                future.get();
            } catch (InterruptedException ex) {
                Logger.getLogger(CouchBaseTest.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ExecutionException ex) {
                Logger.getLogger(CouchBaseTest.class.getName()).log(Level.SEVERE, null, ex);
            }*/
            future.addListener(new OperationCompletionListener() {

                @Override
                public void onComplete(OperationFuture<?> f) throws Exception {
                    countLatch.countDown();
                     //System.out.println(f.get());
                     if (!((Boolean)f.get())) System.out.println("Noway");
                    j.incrementAndGet();

                }
            });
        }
          try {
             countLatch.await();
           } catch (InterruptedException ex) {
            Logger.getLogger(CouchBaseTest.class.getName()).log(Level.SEVERE, null, ex);
           }
   	}
        long stopTime = System.currentTimeMillis();
             System.out.println("value of j" + j.intValue());
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
        System.out.println("stop time after is set is " + stopTime);
        System.out.println("Threads after get are + " + Thread.activeCount());
        Logger.getLogger(CouchBaseTest.class.getName()).log(Level.SEVERE, output.toString());
        client.shutdown();

    }

}

