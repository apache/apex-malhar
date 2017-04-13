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
package com.example.mydtapp;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.db.jdbc.JdbcPOJOInputOperator;
import com.datatorrent.lib.db.jdbc.JdbcStore;
import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.FieldInfo.SupportType;

@ApplicationAnnotation(name = "SimpleJdbcToHDFSApp")
public class JdbcHDFSApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JdbcPOJOInputOperator jdbcInputOperator = dag.addOperator("JdbcInput", new JdbcPOJOInputOperator());
    /**
     * The class given below can be updated to the user defined class based on
     * input table schema The addField infos method needs to be updated
     * accordingly This line can be commented and class can be set from the
     * properties file
     */
   // dag.setOutputPortAttribute(jdbcInputOperator.outputPort, Context.PortContext.TUPLE_CLASS, PojoEvent.class);

    jdbcInputOperator.setFieldInfos(addFieldInfos());

    JdbcStore store = new JdbcStore();
    jdbcInputOperator.setStore(store);

    FileLineOutputOperator fileOutput = dag.addOperator("FileOutputOperator", new FileLineOutputOperator());

    dag.addStream("POJO's", jdbcInputOperator.outputPort, fileOutput.input).setLocality(Locality.CONTAINER_LOCAL);
  }

  /**
   * This method can be modified to have field mappings based on used defined
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
