
package com.datatorrent.contrib.couchbase;

import com.datatorrent.api.Operator;
import com.datatorrent.lib.db.AbstractAggregateTransactionableStoreOutputOperator;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import org.codehaus.jackson.map.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 *
 * @author prerna
 * @param <T>
 */
public abstract class AbstractCouchBaseOutputOperator<T> extends AbstractAggregateTransactionableStoreOutputOperator<T, CouchBaseWindowStore> {
  private static final transient Logger logger = LoggerFactory.getLogger(AbstractCouchBaseOutputOperator.class);
  private final List<T> tuples;
  private transient Operator.ProcessingMode mode;
  public Operator.ProcessingMode getMode()
  {
    return mode;
  }

  public void setMode(Operator.ProcessingMode mode)
  {
    this.mode = mode;
  }

  public AbstractCouchBaseOutputOperator()
  {
    tuples = Lists.newArrayList();
    store = new CouchBaseWindowStore();
  }
  @Override
  public void processTuple(T tuple)
  {
    tuples.add(tuple);
    
    
  }

  
  
  
  public List<T> getTuples()
  {
      return tuples;
  }
  
  @Override
  public void storeAggregate() {
   
      for (T tuple : tuples) {
        insertOrUpdate(tuple);
       
      }
     
    tuples.clear();
  }

  
   public abstract String generatekey(T tuple);
   public abstract Object getObject(T tuple);

   protected abstract void insertOrUpdate(T tuple);
    
}
