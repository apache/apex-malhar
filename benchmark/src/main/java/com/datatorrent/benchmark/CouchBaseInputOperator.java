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
       keys.add("Key1");
       keys.add("Key2");
       keys.add("Key3");
       return keys;
    }
    
}

