package com.datatorrent.benchmark;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.couchbase.CouchBaseWindowStore;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.DAG.Locality;

/**
 *
 * @author prerna
 */


public class CouchBaseApp implements StreamingApplication{
    private final Locality locality = null;
    
    @Override
    public void populateDAG(DAG dag, Configuration conf) {
        int maxValue = 1000;

    RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
    rand.setMinvalue(0);
    rand.setMaxvalue(maxValue);
    rand.setTuplesBlast(200);

    CouchBaseOutput couchbaseOutput = dag.addOperator("couchbaseOuput", new CouchBaseOutput());
    //CouchBaseInputOperator couchbaseInput = dag.addOperator("couchbaseInput", new CouchBaseInputOperator());
   CouchBaseWindowStore store = new CouchBaseWindowStore();
   store.addNodes("node26.morado.com");
   //couchbaseOutput.getStore().addNodes("node26.morado.com"); 
   //couchbaseOutput.getStore().setBucket("default");
   //couchbaseOutput.getStore().setPassword("");
  //couchbaseOutput.getStore().getInstance().set("preru", 12345); 
  // couchbaseOutput.getStore().setKey("abc");
  // couchbaseOutput.getStore().setValue("123");
   //     try {
   //         couchbaseOutput.getStore().connect();
   //     } catch (IOException ex) {
   //         Logger.getLogger(CouchBaseApp.class.getName()).log(Level.SEVERE, null, ex);
     //   }
    couchbaseOutput.setStore(store);
    dag.addStream("ss",rand.integer_data, couchbaseOutput.input).setLocality(locality);
    }

   

}
