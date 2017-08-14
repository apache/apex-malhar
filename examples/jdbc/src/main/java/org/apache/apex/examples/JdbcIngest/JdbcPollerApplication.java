/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.examples.JdbcIngest;

import java.util.List;

import org.apache.apex.malhar.lib.db.jdbc.JdbcPOJOPollInputOperator;
import org.apache.apex.malhar.lib.db.jdbc.JdbcStore;
import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.apex.malhar.lib.util.FieldInfo.SupportType;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "PollJdbcToHDFSApp")
/**
 * @since 3.8.0
 */
public class JdbcPollerApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JdbcPOJOPollInputOperator poller = dag.addOperator("JdbcPoller", new JdbcPOJOPollInputOperator());

    JdbcStore store = new JdbcStore();
    poller.setStore(store);

    poller.setFieldInfos(addFieldInfos());

    FileLineOutputOperator writer = dag.addOperator("Writer", new FileLineOutputOperator());
    dag.setInputPortAttribute(writer.input, PortContext.PARTITION_PARALLEL, true);
    writer.setRotationWindows(60);

    dag.addStream("dbrecords", poller.outputPort, writer.input);
  }

  /**
   * This method can be modified to have field mappings based on user defined
   * class
   */
  private List<FieldInfo> addFieldInfos()
  {
    List<FieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new FieldInfo("ACCOUNT_NO", "accountNumber", SupportType.INTEGER));
    fieldInfos.add(new FieldInfo("NAME", "name", SupportType.STRING));
    fieldInfos.add(new FieldInfo("AMOUNT", "amount", SupportType.INTEGER));
    return fieldInfos;
  }
}
