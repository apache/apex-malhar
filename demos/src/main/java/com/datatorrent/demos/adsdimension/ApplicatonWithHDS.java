package com.datatorrent.demos.adsdimension;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hds.tfile.TFileImpl;
import com.datatorrent.demos.adsdimension.AdInfo.AdInfoAggregator;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

/**
 * An AdsDimensionsDemo run with HDS
 * 
 * Example of configuration
  <pre>
  {@code
  <property>
      <name>dt.application.AdsDimensionsWithHDSDemo.class</name>
      <value>com.datatorrent.demos.adsdimension.ApplicationWithHDS</value>
  </property>
  
  <property>
     <name>dt.application.AdsDimensionsWithHDSDemo.attr.containerMemoryMB</name>
     <value>8192</value>
  </property>

  <property>
     <name>dt.application.AdsDimensionsWithHDSDemo.attr.containerJvmOpts</name>
     <value>-Xmx6g -server -Dlog4j.debug=true -Xloggc:&lt;LOG_DIR&gt;/gc.log -verbose:gc -XX:+PrintGCDateStamps</value>
  </property>

  <property>
     <name>dt.application.AdsDimensionsWithHDSDemo.port.*.attr.QUEUE_CAPACITY</name>
     <value>32000</value>
  </property>

  <property>
       <name>dt.operator.InputGenerator.attr.INITIAL_PARTITION_COUNT</name>
       <value>8</value>
  </property>

  <property>
       <name>dt.operator.DimensionsComputation.attr.APPLICATION_WINDOW_COUNT</name>
       <value>120</value>
  </property>

  <property>
       <name>dt.operator.DimensionsComputation.port.data.attr.PARTITION_PARALLEL</name>
       <value>true</value>
  </property>
  
  <property>
       <name>dt.operator.HDSOut.attr.INITIAL_PARTITION_COUNT</name>
       <value>4</value>
  </property>
  
  <property>
       <name>dt.operator.HDSOut.fileStore.basePath</name>
       <value>AdsDimensionWithHDS</value>
  </property>
  }
</pre>
 *
 */
@ApplicationAnnotation(name="AdsDimensionsWithHDSDemo")
public class ApplicatonWithHDS implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, "AdsDimensionsWithHDSDemo");

    InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);

    DimensionsComputation<AdInfo, AdInfo.AdInfoAggregateEvent> dimensions = dag.addOperator("DimensionsComputation", new DimensionsComputation<AdInfo, AdInfo.AdInfoAggregateEvent>());
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 60);

    String[] dimensionSpecs = new String[] { "time=" + TimeUnit.MINUTES, "time=" + TimeUnit.MINUTES + ":adUnit", "time=" + TimeUnit.MINUTES + ":advertiserId", "time=" + TimeUnit.MINUTES + ":publisherId", "time=" + TimeUnit.MINUTES + ":advertiserId:adUnit", "time=" + TimeUnit.MINUTES + ":publisherId:adUnit", "time=" + TimeUnit.MINUTES + ":publisherId:advertiserId", "time=" + TimeUnit.MINUTES + ":publisherId:advertiserId:adUnit" };

    AdInfoAggregator[] aggregators = new AdInfoAggregator[dimensionSpecs.length];
    for (int i = dimensionSpecs.length; i-- > 0;) {
      AdInfoAggregator aggregator = new AdInfoAggregator();
      aggregator.init(dimensionSpecs[i]);
      aggregators[i] = aggregator;
    }

    dimensions.setAggregators(aggregators);
    DimensionsComputationUnifierImpl unifier = new DimensionsComputationUnifierImpl();
    unifier.setAggregators(aggregators);
    dimensions.setUnifier(unifier);

    HDSOutputOperator hdsOut = dag.addOperator("HDSOut", HDSOutputOperator.class);
    
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    
    hdsOut.setFileStore(hdsFile);
    
    hdsOut.setAggregator(new AdInfoAggregator());
    
    dag.addStream("InputStream", input.outputPort, dimensions.data).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, hdsOut.input);
  }

}
