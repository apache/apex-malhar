package com.datatorrent.benchmark;

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
       for(int i=1;i<100;i++){
       keys.add("Key" + i);
       }
       return keys;
    }
    
}

