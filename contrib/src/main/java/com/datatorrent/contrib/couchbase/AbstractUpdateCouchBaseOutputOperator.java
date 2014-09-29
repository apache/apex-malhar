package com.datatorrent.contrib.couchbase;

import com.datatorrent.common.util.DTThrowable;
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
public abstract class AbstractUpdateCouchBaseOutputOperator<T> extends AbstractCouchBaseOutputOperator<T>
{

  private static final transient Logger logger = LoggerFactory.getLogger(AbstractUpdateCouchBaseOutputOperator.class);

  private transient OperationFuture<Boolean> future;

  @Override
  public void insertOrUpdate(T input)
  {
    String key = generateKey(input);
    Object tuple = getObject(input);
    ObjectMapper mapper = new ObjectMapper();
    String value = new String();
    try {
      value = mapper.writeValueAsString(tuple);
    }
    catch (IOException ex) {
      logger.error("IO Exception", ex);
      DTThrowable.rethrow(ex);
    }

    final CountDownLatch countLatch = new CountDownLatch(store.batch_size);

    future = store.getInstance().add(key, value);
    future.addListener(new OperationCompletionListener()
    {

      @Override
      public void onComplete(OperationFuture<?> f) throws Exception
      {
        countLatch.countDown();
        if (!((Boolean)f.get())) {
          throw new RuntimeException("Operation add failed, Key being added is already present. ");
        }

      }

    });

    if (num_tuples < store.batch_size) {
      try {
        countLatch.await();
      }
      catch (InterruptedException ex) {
        logger.error("Interrupted exception" + ex);
        DTThrowable.rethrow(ex);
      }
    }
  }

}
