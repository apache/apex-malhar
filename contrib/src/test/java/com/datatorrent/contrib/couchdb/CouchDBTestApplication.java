package com.datatorrent.contrib.couchdb;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import org.apache.hadoop.conf.Configuration;

/**
 * An application which tests {@link MapBasedCouchDbOutputOperator}
 * @since 0.3.5
 */
public class CouchDBTestApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    CouchTupleGenerator tupleGenerator = dag.addOperator("JsonGenerator", CouchTupleGenerator.class);
    MapBasedCouchDbOutputOperator dbOutputOperator = dag.addOperator("Db-Writer", MapBasedCouchDbOutputOperator.class);
    dbOutputOperator.setDatabase(CouchDBTestHelper.get().getDatabase());
    dbOutputOperator.setUpdateRevisionWhenNull(true);

    dag.addStream("generatorToWriter", tupleGenerator.outputPort, dbOutputOperator.inputPort);
  }
}
