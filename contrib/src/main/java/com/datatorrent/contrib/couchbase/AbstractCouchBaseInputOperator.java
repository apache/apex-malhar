package com.datatorrent.contrib.couchbase;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.AbstractStoreInputOperator;
import java.io.IOException;
import java.util.ArrayList;

/**
 *
 * @author prerna
 * @param <T>
 */
public abstract class AbstractCouchBaseInputOperator<T> extends AbstractStoreInputOperator<T, CouchBaseStore>
{

  public AbstractCouchBaseInputOperator()
  {
    store = new CouchBaseStore();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void emitTuples()
  {
    ArrayList<String> keys = getKeys();
    for (int i = 0; i < keys.size(); i++) {
      try {
        // Return the result
        Object result = store.getInstance().get(keys.get(i));
        T tuple = getTuple(result);
        outputPort.emit(tuple);

      }
      catch (Exception ex) {
        try {
          store.disconnect();
        }
        catch (IOException ex1) {
          DTThrowable.rethrow(ex1);
        }
        DTThrowable.rethrow(ex);
      }
    }

  }

  public abstract T getTuple(Object object);

  public abstract ArrayList<String> getKeys();

}
