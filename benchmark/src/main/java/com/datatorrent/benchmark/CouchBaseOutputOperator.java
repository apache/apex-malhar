
package com.datatorrent.benchmark;

import com.datatorrent.contrib.couchbase.AbstractInsertCouchBaseOutputOperator;

public class CouchBaseOutputOperator extends AbstractInsertCouchBaseOutputOperator<Integer>{


    @Override
    public String generateKey(Integer tuple) {
       return "Key" + tuple;
    }

    @Override
    public Object getObject(Integer tuple) {
      tuple = 500;
      return tuple;
    }




}
