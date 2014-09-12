 
package com.datatorrent.benchmark;

import com.datatorrent.contrib.couchbase.AbstractInsertCouchBaseOutputOperator;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author prerna
 */


public class CouchBaseOutputOperator extends AbstractInsertCouchBaseOutputOperator<Integer>{


    @Override
    public String generatekey(Integer tuple) {
       return "abc";
    }

    @Override
    public Object getObject(Integer tuple) {
      tuple = 500;
       return tuple;
    }

    
    
    
}
