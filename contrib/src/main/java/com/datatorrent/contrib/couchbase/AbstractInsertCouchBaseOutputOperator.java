package com.datatorrent.contrib.couchbase;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author prerna
 */
public abstract class AbstractInsertCouchBaseOutputOperator<T> extends AbstractCouchBaseOutputOperator<T> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractInsertCouchBaseOutputOperator.class);
   
    @Override
    public void insertOrUpdate(T input) {

        String key = generatekey(input);
        Object tuple = getObject(input);
        ObjectMapper mapper = new ObjectMapper();
        String value = new String();
        try {
            value = mapper.writeValueAsString(tuple);
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(AbstractInsertCouchBaseOutputOperator.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        int numOfBatches = store.getMax_tuples() / store.getBatch_size();
        for (int k = 0; k < numOfBatches; k++) {
        final CountDownLatch countLatch = new CountDownLatch(store.batch_size);
        for (int i = 0; i < store.batch_size; i++) {
            
            final OperationFuture<Boolean> future = store.getInstance().set(key, value);
            future.addListener(new OperationCompletionListener() {

                @Override
                public void onComplete(OperationFuture<?> f) throws Exception {
                    countLatch.countDown();
                     //System.out.println(f.get());
                     if (!((Boolean)f.get())) 
                         System.out.println("Noway");
                  

                }
            });
        }
          try {
             countLatch.await();
           } catch (InterruptedException ex) {
            java.util.logging.Logger.getLogger(AbstractInsertCouchBaseOutputOperator.class.getName()).log(Level.SEVERE, null, ex);
           }
   	}

    }

}
