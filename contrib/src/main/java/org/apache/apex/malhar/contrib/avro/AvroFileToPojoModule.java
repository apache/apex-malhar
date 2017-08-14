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
package org.apache.apex.malhar.contrib.avro;

import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;

/**
 * <p>
 * Avro File To Pojo Module
 * </p>
 * This module emits Pojo based on the schema derived from the
 * input file<br>
 *
 * Example of how to configure and add this module to DAG
 *
 * AvroFileToPojoModule avroFileToPojoModule = new AvroFileToPojoModule();
 * avroFileToPojoModule.setPojoClass([className.class]);
 * avroFileToPojoModule.setAvroFileDirectory(conf.get("[configuration property]", "[default file directory]"));
 * avroFileToPojoModule = dag.addModule("avroFileToPojoModule", avroFileToPojoModule);
 *
 * No need to provide schema,its inferred from the file<br>
 *
 * Users can add the {@link FSWindowDataManager}
 * to ensure exactly once semantics with a HDFS backed WAL.
 *
 * @displayName AvroFileToPojoModule
 * @category Input
 * @tags fs, file,avro, input operator, generic record, pojo
 *
 * @since
 */
public class AvroFileToPojoModule implements Module
{
  public final transient ProxyOutputPort<Object> output = new ProxyOutputPort<>();
  public final transient ProxyOutputPort<GenericRecord> errorPort = new ProxyOutputPort<>();
  //output ports from AvroFileInputOperator
  public final transient ProxyOutputPort<String> completedAvroFilesPort = new ProxyOutputPort<>();
  public final transient ProxyOutputPort<String> avroErrorRecordsPort = new ProxyOutputPort<>();

  private AvroFileInputOperator avroFileInputOperator = new AvroFileInputOperator();
  Class<?> pojoClass = null;

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    AvroFileInputOperator avroFileInputOperator = dag.addOperator("AvroFileInputOperator", this.avroFileInputOperator);
    AvroToPojo avroToPojo = dag.addOperator("AvroGenericObjectToPojo", new AvroToPojo());

    dag.setOutputPortAttribute(avroToPojo.output, Context.PortContext.TUPLE_CLASS, pojoClass);

    dag.addStream("avroFileContainerToPojo", avroFileInputOperator.output, avroToPojo.data)
        .setLocality(DAG.Locality.CONTAINER_LOCAL);

    output.set(avroToPojo.output);
    errorPort.set(avroToPojo.errorPort);

    completedAvroFilesPort.set(avroFileInputOperator.completedFilesPort);
    avroErrorRecordsPort.set(avroFileInputOperator.errorRecordsPort);
  }

  public void setPojoClass(Class<?> pojoClass)
  {
    this.pojoClass = pojoClass;
  }

  public void setAvroFileDirectory(String directory)
  {
    avroFileInputOperator.setDirectory(directory);
  }
}
