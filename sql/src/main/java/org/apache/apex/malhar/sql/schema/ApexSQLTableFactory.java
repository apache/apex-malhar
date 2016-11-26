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
package org.apache.apex.malhar.sql.schema;

import java.util.Map;

import org.apache.apex.malhar.sql.table.CSVMessageFormat;
import org.apache.apex.malhar.sql.table.Endpoint;
import org.apache.apex.malhar.sql.table.FileEndpoint;
import org.apache.apex.malhar.sql.table.KafkaEndpoint;
import org.apache.apex.malhar.sql.table.MessageFormat;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceStability.Evolving
/**
 * @since 3.6.0
 */
public class ApexSQLTableFactory implements TableFactory<Table>
{
  @SuppressWarnings("unchecked")
  @Override
  public Table create(SchemaPlus schemaPlus, String name, Map<String, Object> operands, RelDataType rowType)
  {
    Endpoint endpoint;
    String endpointSystemType = (String)operands.get(Endpoint.ENDPOINT);

    if (endpointSystemType.equalsIgnoreCase(Endpoint.EndpointType.FILE.name())) {
      endpoint = new FileEndpoint();
    } else if (endpointSystemType.equalsIgnoreCase(Endpoint.EndpointType.KAFKA.name())) {
      endpoint = new KafkaEndpoint();
    } else {
      throw new RuntimeException("Cannot find endpoint");
    }
    endpoint.setEndpointOperands((Map<String, Object>)operands.get(Endpoint.SYSTEM_OPERANDS));

    MessageFormat mf;
    String messageFormat = (String)operands.get(MessageFormat.MESSAGE_FORMAT);
    if (messageFormat.equalsIgnoreCase(MessageFormat.MessageFormatType.CSV.name())) {
      mf = new CSVMessageFormat();
    } else {
      throw new RuntimeException("Cannot find message format");
    }
    mf.setMessageFormatOperands((Map<String, Object>)operands.get(MessageFormat.MESSAGE_FORMAT_OPERANDS));

    endpoint.setMessageFormat(mf);

    return new ApexSQLTable(schemaPlus, name, operands, rowType, endpoint);
  }
}
