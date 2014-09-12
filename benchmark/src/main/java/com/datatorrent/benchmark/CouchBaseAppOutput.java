package com.datatorrent.benchmark;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 *
 * @author prerna
 */
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

        CouchBaseOutputOperator couchbaseOutput = dag.addOperator("couchbaseOuput", new CouchBaseOutputOperator());

        dag.addStream("ss", rand.integer_data, couchbaseOutput.input).setLocality(locality);
    }

}
