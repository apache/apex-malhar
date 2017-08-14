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
package org.apache.apex.malhar.lib.db.jdbc;

import java.sql.Types;
import java.util.List;

import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.apex.malhar.lib.util.FieldInfo.SupportType;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "JdbcToJdbcApp")
public class JdbcIOApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JdbcPOJOInputOperator jdbcInputOperator = dag.addOperator("JdbcInput", new JdbcPOJOInputOperator());
    JdbcStore store = new JdbcStore();
    store.setDatabaseDriver("org.hsqldb.jdbcDriver");
    store.setDatabaseUrl("jdbc:hsqldb:mem:test");
    jdbcInputOperator.setStore(store);
    jdbcInputOperator.setFieldInfos(addFieldInfos());
    jdbcInputOperator.setFetchSize(10);
    jdbcInputOperator.setTableName("test_app_event_table");
    dag.getMeta(jdbcInputOperator).getMeta(jdbcInputOperator.outputPort).getAttributes()
        .put(Context.PortContext.TUPLE_CLASS, JdbcIOAppTest.PojoEvent.class);

    JdbcPOJOInsertOutputOperator jdbcOutputOperator = dag.addOperator("JdbcOutput", new JdbcPOJOInsertOutputOperator());
    JdbcTransactionalStore outputStore = new JdbcTransactionalStore();
    outputStore.setDatabaseDriver("org.hsqldb.jdbcDriver");
    outputStore.setDatabaseUrl("jdbc:hsqldb:mem:test");
    jdbcOutputOperator.setStore(outputStore);
    jdbcOutputOperator.setFieldInfos(addJdbcFieldInfos());
    jdbcOutputOperator.setTablename("test_app_output_event_table");
    jdbcOutputOperator.setBatchSize(10);
    dag.getMeta(jdbcOutputOperator).getMeta(jdbcOutputOperator.input).getAttributes()
        .put(Context.PortContext.TUPLE_CLASS, JdbcIOAppTest.PojoEvent.class);

    dag.addStream("POJO's", jdbcInputOperator.outputPort, jdbcOutputOperator.input)
        .setLocality(Locality.CONTAINER_LOCAL);
  }

  private List<FieldInfo> addFieldInfos()
  {
    List<FieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new FieldInfo("ACCOUNT_NO", "accountNumber", SupportType.INTEGER));
    fieldInfos.add(new FieldInfo("NAME", "name", SupportType.STRING));
    fieldInfos.add(new FieldInfo("AMOUNT", "amount", SupportType.INTEGER));
    return fieldInfos;
  }

  private List<JdbcFieldInfo> addJdbcFieldInfos()
  {
    List<JdbcFieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new JdbcFieldInfo("ACCOUNT_NO", "accountNumber", SupportType.INTEGER, Types.INTEGER));
    fieldInfos.add(new JdbcFieldInfo("NAME", "name", SupportType.STRING, Types.VARCHAR));
    fieldInfos.add(new JdbcFieldInfo("AMOUNT", "amount", SupportType.INTEGER, Types.INTEGER));
    return fieldInfos;
  }

}
