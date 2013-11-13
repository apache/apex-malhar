/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
