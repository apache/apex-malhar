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
import static com.datatorrent.benchmark.ApplicationFixed.QUEUE_CAPACITY;
import com.datatorrent.api.Context;

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
    
    CouchBaseInputOperator couchbaseInput = dag.addOperator("couchbaseInput", CouchBaseInputOperator.class);
    couchbaseInput.getStore().addNodes("node26.morado.com"); 
    couchbaseInput.getStore().setBucket("default");
    couchbaseInput.getStore().setPassword("");
    WordCountOperator<String> counter = dag.addOperator("Counter", new WordCountOperator<String>());

    dag.addStream("Generator2Counter",couchbaseInput.outputPort,counter.input ).setLocality(locality);
    }

   

}
