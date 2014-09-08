
package com.datatorrent.benchmark;

import com.datatorrent.contrib.couchbase.CouchBaseStore;
import com.datatorrent.lib.db.AbstractStoreInputOperator;

/**
 *
 * @author prerna
 */


public class CouchBaseInputOperator extends AbstractStoreInputOperator<String, CouchBaseStore>{

    @Override
    public void emitTuples() {
       // to be inserted
    }
    
}
