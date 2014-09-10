
package com.datatorrent.benchmark;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.couchbase.AbstractCouchBaseInputOperator;
import java.util.ArrayList;

/**
 *  *
 *   * @author prerna
 *    */


public class CouchBaseInputOperator extends AbstractCouchBaseInputOperator<String>{
    
    @Override
    public String getTuple(Object object) {
       return object.toString();
    }

    @Override
    public ArrayList<String> getKeys() {
       ArrayList<String> keys = new ArrayList<String>();
       keys.add("prerna");
       keys.add("key1");
       return keys;
    }
    
}

