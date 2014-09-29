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
       keys.add("Key0");
       keys.add("Key1");
       keys.add("Key10");
       keys.add("Key100");
       keys.add("Key1000");
       keys.add("Key10000");
       keys.add("Key100000");
       return keys;
    }
    
}

