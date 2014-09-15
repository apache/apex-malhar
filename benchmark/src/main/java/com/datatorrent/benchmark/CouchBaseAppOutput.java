package com.datatorrent.benchmark;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  *
 *   * @author prerna
 *    */
@ApplicationAnnotation(name="CouchBaseAppOutput")
public class CouchBaseAppOutput implements StreamingApplication {

    private final Locality locality = null;

    @Override
    public void populateDAG(DAG dag, Configuration conf) {
        int maxValue = 1000;
           
        RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
        rand.setMinvalue(0);
        rand.setMaxvalue(maxValue);
        rand.setTuplesBlast(200);
        CouchBaseOutputOperator couchbaseOutputTest = new CouchBaseOutputOperator();
        couchbaseOutputTest.getStore().setBucket("default");
        couchbaseOutputTest.getStore().setPassword("");
        couchbaseOutputTest.getStore().setUriString("node26.morado.com:8091");
        try {
            couchbaseOutputTest.getStore().connect();
        } catch (IOException ex) {
            Logger.getLogger(CouchBaseAppOutput.class.getName()).log(Level.SEVERE, null, ex);
        }
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
         couchbaseOutputTest.insertOrUpdate(i);
         }
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println(elapsedTime);
        //CouchBaseOutputOperator couchbaseOutput = dag.addOperator("couchbaseOuput", new CouchBaseOutputOperator());
        //couchbaseOutput.getStore().setBucket("default");
        //couchbaseOutput.getStore().setPassword("");
        //dag.addStream("ss", rand.integer_data, couchbaseOutput.input).setLocality(locality);
    }

}

