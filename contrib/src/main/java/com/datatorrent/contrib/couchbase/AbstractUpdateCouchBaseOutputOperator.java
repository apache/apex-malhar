package com.datatorrent.contrib.couchbase;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
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
public abstract class AbstractUpdateCouchBaseOutputOperator<T> extends AbstractCouchBaseOutputOperator<T> {

    private static final transient Logger logger = LoggerFactory.getLogger(AbstractUpdateCouchBaseOutputOperator.class);
    private int expireTime;
    private transient OperationFuture<Boolean> future;

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

        final CountDownLatch countLatch = new CountDownLatch(store.batch_size);

        future = store.getInstance().add(key, value);
        future.addListener(new OperationCompletionListener() {

            @Override
            public void onComplete(OperationFuture<?> f) throws Exception {
                countLatch.countDown();
                if (!((Boolean) f.get())) {
                    throw new RuntimeException("Operation add failed, Key being added is already present. ");
                }

            }
        });

        if (num_tuples < store.batch_size) {
            try {
                countLatch.await();
            } catch (InterruptedException ex) {
                java.util.logging.Logger.getLogger(AbstractInsertCouchBaseOutputOperator.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public int getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(int expireTime) {
        this.expireTime = expireTime;
    }

}
