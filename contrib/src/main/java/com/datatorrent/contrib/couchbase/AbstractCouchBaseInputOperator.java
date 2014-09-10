package com.datatorrent.contrib.couchbase;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.AbstractStoreInputOperator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author prerna
 * @param <T>
 */
public abstract class AbstractCouchBaseInputOperator<T> extends AbstractStoreInputOperator<T, CouchBaseStore> {

    private static final transient Logger logger = LoggerFactory.getLogger(AbstractCouchBaseInputOperator.class);
     /**
     * Any concrete class has to override this method to convert a Database row
     * into Tuple.
     *
     * @param row a single row that has been read from database.
     * @return Tuple a tuples created from row which can be any Java object.
     */
 // public abstract T getTuple(Row row);
    /**
     * Any concrete class has to override this method to return the query string
     * which will be used to retrieve data from database.
     *
     * @return Query string
     */
   // public abstract String queryToRetrieveData();

    /**
     * The output port that will emit tuple into DAG.
     */
  //@OutputPortFieldAnnotation(name = "outputPort")
    //public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();
    /**
     * This executes the query to retrieve result from database. It then
     * converts each row into tuple and emit that into output port.
     */
    
    public AbstractCouchBaseInputOperator() {
    store = new CouchBaseStore();
  }
    @Override
    public void emitTuples() {
       // String query = queryToRetrieveData();
       // logger.debug(String.format("select statement: %s", query));
       
        ArrayList<String> keys = getKeys();
        for(int i=0;i<keys.size();i++)
        {
         try {
           
            // Return the result 
            Object result = store.getInstance().get(keys.get(i));
            
            T tuple = getTuple(result);
            outputPort.emit(tuple);

        } catch (Exception ex) {
            try {
                store.disconnect();
            } catch (IOException ex1) {
                java.util.logging.Logger.getLogger(AbstractCouchBaseInputOperator.class.getName()).log(Level.SEVERE, null, ex1);
            }
            DTThrowable.rethrow(ex);
        }
        }

    }
      public abstract T getTuple(Object object);
      public abstract ArrayList<String> getKeys();
}
