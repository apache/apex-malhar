 
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

    
    public void insertOrUpdate(Integer tuple) {
       tuple = 700;
       this.insertOrUpdate(tuple);
    }


 @Override
    public void beginWindow(long windowId)
    {
        super.beginWindow(windowId);
        java.util.logging.Logger.getLogger(AbstractInsertCouchBaseOutputOperator.class.getName()).log(Level.SEVERE,"In begin window");

    }

  @Override
  public void endWindow() {
         super.endWindow();
         java.util.logging.Logger.getLogger(AbstractInsertCouchBaseOutputOperator.class.getName()).log(Level.SEVERE,"In end window" );
   }

   @Override
  public void teardown()
  {
     java.util.logging.Logger.getLogger(CouchBaseOutputOperator.class.getName()).log(Level.SEVERE, "In teardown");
     super.teardown();
     try {
            store.disconnect();
           java.util.logging.Logger.getLogger(CouchBaseOutputOperator.class.getName()).log(Level.SEVERE, "Diconnect called");
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(CouchBaseOutputOperator.class.getName()).log(Level.SEVERE, null, ex);
        }
  }


    
    
    
}
