 
package com.datatorrent.benchmark;

import com.datatorrent.contrib.couchbase.AbstractInsertCouchBaseOutputOperator;

/**
 *
 * @author prerna
 */


public class CouchBaseOutputOperator extends AbstractInsertCouchBaseOutputOperator<Integer>{


    @Override
    public String generatekey(Integer tuple) {
       return "Key" + tuple;
    }

    @Override
    public Object getObject(Integer tuple) {
      tuple = 500;
      return tuple;
    }

    
    
    
}
