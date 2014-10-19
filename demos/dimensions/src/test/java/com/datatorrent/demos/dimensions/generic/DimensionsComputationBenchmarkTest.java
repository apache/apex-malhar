package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.LocalMode;
import com.datatorrent.lib.util.ObjectMapperString;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DimensionsComputationBenchmarkTest
{
  private static final Logger LOG = LoggerFactory.getLogger(DimensionsComputationBenchmarkTest.class);

  @Test
  public void test() throws Exception
  {
    DimensionsComputationBenchmark app = new DimensionsComputationBenchmark();
    LocalMode lma = LocalMode.newInstance();
    app.populateDAG(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    ObjectMapper propertyObjectMapper = new ObjectMapper();
    propertyObjectMapper.configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, true);
    propertyObjectMapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);

    // Test that DAG can be serialized to JSON by by Gateway
    ObjectMapperString str = new ObjectMapperString(
            propertyObjectMapper.writeValueAsString(
                    lma.getDAG().getOperatorMeta("DimensionsComputation").getOperator()));
    LOG.debug("DimensionsComputation representation: {}", str);


    lc.run(10000);


  }


}
