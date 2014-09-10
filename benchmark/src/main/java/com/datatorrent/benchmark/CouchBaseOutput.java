
package com.datatorrent.benchmark;

import com.datatorrent.contrib.couchbase.AbstractInsertCouchBaseOutputOperator;
import com.datatorrent.contrib.couchbase.CouchBaseWindowStore;
import com.datatorrent.lib.db.AbstractAggregateTransactionableStoreOutputOperator;
import java.io.IOException;
import java.util.logging.Level;

/**
 *  *
 *   * @author prerna
 *    */


public class CouchBaseOutput extends AbstractInsertCouchBaseOutputOperator<Integer>{

    @Override
    public String generatekey(Integer tuple) {
        return "abc";
    }

    @Override
    public Object getObject(Integer tuple) {
       tuple = 500;
       return tuple;
    }

   
    
    @Override
    public void endWindow()
    {
        java.util.logging.Logger.getLogger(CouchBaseOutput.class.getName()).log(Level.SEVERE,"In end window");
         super.endWindow();
    }
    
    @Override
    public void beginWindow(long windowId)
    {
        java.util.logging.Logger.getLogger(CouchBaseOutput.class.getName()).log(Level.SEVERE,"In begin window");
        super.beginWindow(windowId);
        
    }
    
    public void teardown(){
         java.util.logging.Logger.getLogger(CouchBaseOutput.class.getName()).log(Level.SEVERE, "In teardown");
         super.teardown();
        try {
            store.disconnect();
            
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(CouchBaseOutput.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
  
}

