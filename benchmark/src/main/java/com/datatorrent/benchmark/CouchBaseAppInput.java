package com.datatorrent.benchmark;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 *
 * @author prerna
 */
@ApplicationAnnotation(name="CouchBaseAppInput")
public class CouchBaseAppInput implements StreamingApplication {

    private final Locality locality = null;

    @Override
    public void populateDAG(DAG dag, Configuration conf) {
        CouchBaseInputOperator couchbaseInput = dag.addOperator("couchbaseInput", CouchBaseInputOperator.class);
        //couchbaseInput.getStore().setBucket("default");
        //couchbaseInput.getStore().setPassword("");
        WordCountOperator<String> counter = dag.addOperator("Counter", new WordCountOperator<String>());

        dag.addStream("Generator2Counter", couchbaseInput.outputPort, counter.input).setLocality(locality);
    }

}
