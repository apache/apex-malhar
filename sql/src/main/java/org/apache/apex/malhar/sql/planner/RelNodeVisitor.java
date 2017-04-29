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
package org.apache.apex.malhar.sql.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.apex.malhar.sql.operators.OperatorUtils;
import org.apache.apex.malhar.sql.schema.TupleSchemaRegistry;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.RelNode;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;

/**
 * This class is the main class that converts relational algebra to a sub-DAG.
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public class RelNodeVisitor
{
  private final DAG dag;
  private final TupleSchemaRegistry tupleSchemaRegistry;
  private final JavaTypeFactory typeFactory;

  public RelNodeVisitor(DAG dag, JavaTypeFactory typeFactory)
  {
    this.dag = dag;
    this.typeFactory = typeFactory;
    this.tupleSchemaRegistry = new TupleSchemaRegistry();
  }

  /**
   * This is the main method in this relational node visitor which traverses the relational algebra in reverse direction
   * and populate the given underlying DAG object.
   *
   * @param relNode RelNode which needs to be traversed.
   *
   * @return RelInfo representing information of current stage
   * @throws Exception
   */
  public final RelInfo traverse(RelNode relNode) throws Exception
  {
    List<RelInfo> inputStreams = new ArrayList<>();
    for (RelNode input : relNode.getInputs()) {
      inputStreams.add(traverse(input));
    }

    ApexRelNode.RelContext relContext = new ApexRelNode.RelContext(dag, typeFactory, tupleSchemaRegistry);

    RelInfo currentNodeRelInfo;
    ApexRelNode apexRelNode = ApexRelNode.relNodeMapping.get(relNode.getClass());
    if (apexRelNode == null) {
      throw new UnsupportedOperationException("RelNode " + relNode.getRelTypeName() + " is not supported.");
    }
    currentNodeRelInfo = apexRelNode.visit(relContext, relNode, inputStreams);

    if (currentNodeRelInfo != null && inputStreams.size() != 0) {
      for (int i = 0; i < inputStreams.size(); i++) {
        RelInfo inputStream = inputStreams.get(i);
        Operator.OutputPort outputPort = inputStream.getOutPort();
        Operator.InputPort inputPort = currentNodeRelInfo.getInputPorts().get(i);

        String streamName = OperatorUtils.getUniqueStreamName(inputStream.getRelName(),
            currentNodeRelInfo.getRelName());
        Class schema;
        if (inputStream.getOutRelDataType() != null) {
          schema = TupleSchemaRegistry.getSchemaForRelDataType(tupleSchemaRegistry, streamName,
              inputStream.getOutRelDataType());
        } else if (inputStream.getClazz() != null) {
          schema = inputStream.getClazz();
        } else {
          throw new RuntimeException("Unexpected condition reached.");
        }
        dag.setOutputPortAttribute(outputPort, Context.PortContext.TUPLE_CLASS, schema);
        dag.setInputPortAttribute(inputPort, Context.PortContext.TUPLE_CLASS, schema);
        dag.addStream(streamName, outputPort, inputPort);
      }
    }

    if (currentNodeRelInfo.getOutPort() == null) {
      // End of the pipeline.
      String schemaJar = tupleSchemaRegistry.generateCommonJar();

      String jars = dag.getAttributes().get(Context.DAGContext.LIBRARY_JARS);
      dag.setAttribute(Context.DAGContext.LIBRARY_JARS,
          ((jars != null) && (jars.length() != 0)) ? jars + "," + schemaJar : schemaJar);
    }

    return currentNodeRelInfo;
  }
}
