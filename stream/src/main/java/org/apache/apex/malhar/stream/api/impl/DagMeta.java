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
package org.apache.apex.malhar.stream.api.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.stream.api.Option;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * Logical graph data structure for DAG <br>
 *
 * With the build method({@link #buildDAG()}, {@link #buildDAG(DAG)}) to convert it to Apex DAG
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public class DagMeta
{
  private List<NodeMeta> heads = new LinkedList<>();

  List<Pair<Attribute, Object>> dagAttributes = new LinkedList<>();

  public static class NodeMeta
  {

    private Operator operator;

    private Option[] options;

    List<Pair<Attribute, Object>> operatorAttributes = new LinkedList<>();

    private Map<Operator.OutputPort, Pair<List<Operator.InputPort>, DAG.Locality>> nodeStreams = new HashMap<>();

    Map<Operator.OutputPort, List<Pair<Attribute, Object>>> outputPortAttributes = new HashMap<>();

    Map<Operator.InputPort, List<Pair<Attribute, Object>>> inputPortAttributes = new HashMap<>();

    private Set<Operator.InputPort> operatorInputs = new HashSet<>();

    private List<NodeMeta> children = new LinkedList<>();

    private List<NodeMeta> parent = new LinkedList<>();

    public List<NodeMeta> getParent()
    {
      return parent;
    }

    public List<NodeMeta> getChildren()
    {
      return children;
    }

    public Operator getOperator()
    {
      return operator;
    }

    public Map<Operator.OutputPort, Pair<List<Operator.InputPort>, DAG.Locality>> getNodeStreams()
    {
      return nodeStreams;
    }

    public NodeMeta(Operator operator, Option... options)
    {

      this.operator = operator;

      this.options = options;

      for (Field field : this.operator.getClass().getFields()) {
        int modifier = field.getModifiers();
        if (Modifier.isPublic(modifier) && Modifier.isTransient(modifier) &&
            Operator.OutputPort.class.isAssignableFrom(field.getType())) {
          try {
            nodeStreams.put((Operator.OutputPort)field.get(operator), MutablePair.<List<Operator.InputPort>, DAG.Locality>of(new LinkedList<Operator.InputPort>(), null));
          } catch (IllegalAccessException e) {
            //Do nothing because it's been checked in if condition
          }
        }
        if (Modifier.isPublic(modifier) && Modifier.isTransient(modifier) &&
            Operator.InputPort.class.isAssignableFrom(field.getType())) {
          try {
            operatorInputs.add((Operator.InputPort)field.get(operator));
          } catch (IllegalAccessException e) {
            //Do nothing because it's been checked in if condition
          }
        }
      }
    }

    public String getOperatorName()
    {
      for (Option opt : options) {
        if (opt instanceof Option.OpName) {
          return ((Option.OpName)opt).getName();
        }
      }
      return operator.toString();
    }
  }

  public DagMeta()
  {

  }

  public DAG buildDAG()
  {
    DAG dag = new LogicalPlan();
    buildDAG(dag);
    return dag;
  }

  public void buildDAG(DAG dag)
  {
    for (NodeMeta nm : heads) {
      visitNode(nm, dag);
    }
    logger.debug("Finish building the dag:\n {}", dag.toString());
  }

  private void visitNode(NodeMeta nm, DAG dag)
  {
    String opName = nm.getOperatorName();
    logger.debug("Building DAG: add operator {}: {}", opName, nm.operator);
    dag.addOperator(opName, nm.operator);

    for (NodeMeta child : nm.children) {
      visitNode(child, dag);
    }

    for (Map.Entry<Operator.OutputPort, Pair<List<Operator.InputPort>, DAG.Locality>> entry : nm.nodeStreams.entrySet()) {
      if (entry.getKey() == null || entry.getValue().getKey() == null || 0 == entry.getValue().getKey().size()) {
        continue;
      }
      logger.debug("Building DAG: add stream {} from {} to {}", entry.getKey().toString(), entry.getKey(), entry.getValue().getLeft().toArray(new Operator.InputPort[]{}));
      DAG.StreamMeta streamMeta = dag.addStream(entry.getKey().toString(), entry.getKey(),
          entry.getValue().getLeft().toArray(new Operator.InputPort[]{}));
      // set locality
      if (entry.getValue().getRight() != null) {
        logger.debug("Building DAG: set locality of the stream {} to {}", entry.getKey().toString(), entry.getValue().getRight());
        streamMeta.setLocality(entry.getValue().getRight());
      }
      //set attributes for output port
      if (nm.outputPortAttributes.containsKey(entry.getKey())) {
        for (Pair<Attribute, Object> attr : nm.outputPortAttributes.get(entry.getKey())) {
          logger.debug("Building DAG: set port attribute {} to {} for port {}", attr.getLeft(), attr.getValue(), entry.getKey());
          dag.setOutputPortAttribute(entry.getKey(), attr.getLeft(), attr.getValue());
        }
      }
    }


    for (Operator.InputPort input : nm.operatorInputs) {
      //set input port attributes
      if (nm.inputPortAttributes.containsKey(input)) {
        for (Pair<Attribute, Object> attr : nm.inputPortAttributes.get(input)) {
          logger.debug("Building DAG: set port attribute {} to {} for port {}", attr.getLeft(), attr.getValue(), input);
          dag.setInputPortAttribute(input, attr.getLeft(), attr.getValue());
        }
      }
    }

    // set operator attributes
    for (Pair<Attribute, Object> attr : nm.operatorAttributes) {
      logger.debug("Building DAG: set operator attribute {} to {} for operator {}", attr.getLeft(), attr.getValue(), nm.operator);
      dag.setAttribute(nm.operator, attr.getLeft(), attr.getValue());
    }

  }

  public NodeMeta addNode(Operator operator, NodeMeta parent, Operator.OutputPort parentOutput, Operator.InputPort inputPort, Option... options)
  {

    NodeMeta newNode = new NodeMeta(operator, options);
    if (parent == null) {
      heads.add(newNode);
    } else {
      parent.nodeStreams.get(parentOutput).getLeft().add(inputPort);
      parent.children.add(newNode);
      newNode.parent.add(parent);
    }
    return newNode;
  }

  private static final Logger logger = LoggerFactory.getLogger(DagMeta.class);

}
