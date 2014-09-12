package com.datatorrent.contrib.couchbase;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.AbstractStoreInputOperator;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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

    public AbstractCouchBaseInputOperator() {
        store = new CouchBaseStore();
    }

    //@Override
    public void setup(Context.OperatorContext context) {
       /* URI uri = null;
        store.URIs.add("node26.morado.com:8091");
        for (String url : store.URIs) {
            try {

                uri = new URI("http", url, "/pools", null, null);
            } catch (URISyntaxException ex) {
                java.util.logging.Logger.getLogger(AbstractCouchBaseOutputOperator.class.getName()).log(Level.SEVERE, null, ex);
            }
            store.addNodes(uri);
        }*/
        super.setup(context);
    }

    @Override
    public void emitTuples() {
        ArrayList<String> keys = getKeys();
        for (int i = 0; i < keys.size(); i++) {
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
