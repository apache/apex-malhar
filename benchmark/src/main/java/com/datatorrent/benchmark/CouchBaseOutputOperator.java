 
package com.datatorrent.benchmark;

import com.datatorrent.contrib.couchbase.AbstractCouchBaseOutputOperator;
import com.datatorrent.contrib.couchbase.CouchBaseWindowStore;
import com.datatorrent.lib.db.AbstractAggregateTransactionableStoreOutputOperator;

/**
 *
 * @author prerna
 */


public class CouchBaseOutputOperator extends AbstractCouchBaseOutputOperator<String>{

    @Override
    public String generatekey(String tuple) {
       return store.getKey();
    }

    @Override
    public Object getObject(String tuple) {
        return store.getValue();
    }

    @Override
    protected void insertOrUpdate(String tuple) {
       
    }

    
    
    
}
