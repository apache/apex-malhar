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

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * Graph data structure for DAG
 * With this data structure, the framework can do lazy load and optimization
 *
 * @since 3.4.0
 */
public class DagMeta
{

  private List<NodeMeta> heads = new LinkedList<>();

  List<Pair<Attribute, Object>> dagAttributes = new LinkedList<>();

  public static class NodeMeta
  {

    private String nodeName;

    private Operator operator;

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

    public String getNodeName()
    {
      return nodeName;
    }

    public Operator getOperator()
    {
      return operator;
    }

    public Map<Operator.OutputPort, Pair<List<Operator.InputPort>, DAG.Locality>> getNodeStreams()
    {
      return nodeStreams;
    }

    public NodeMeta(Operator operator, String nodeName)
    {

      this.nodeName = nodeName;

      this.operator = operator;

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
  }

  private void visitNode(NodeMeta nm, DAG dag)
  {
    dag.addOperator(nm.nodeName, nm.operator);
    for (NodeMeta child : nm.children) {
      visitNode(child, dag);
    }

    for (Map.Entry<Operator.OutputPort, Pair<List<Operator.InputPort>, DAG.Locality>> entry : nm.nodeStreams.entrySet()) {
      if (entry.getKey() == null || entry.getValue().getKey() == null || 0 == entry.getValue().getKey().size()) {
        continue;
      }
      DAG.StreamMeta streamMeta = dag.addStream(entry.getKey().toString(), entry.getKey(),
          entry.getValue().getLeft().toArray(new Operator.InputPort[]{}));
      // set locality
      if (entry.getValue().getRight() != null) {
        streamMeta.setLocality(entry.getValue().getRight());
      }
      //set attributes for output port
      if (nm.outputPortAttributes.containsKey(entry.getKey())) {
        for (Pair<Attribute, Object> attr : nm.outputPortAttributes.get(entry.getKey())) {
          dag.setOutputPortAttribute(entry.getKey(), attr.getLeft(), attr.getValue());
        }
      }
    }


    for (Operator.InputPort input : nm.operatorInputs) {
      //set input port attributes
      if (nm.inputPortAttributes.containsKey(input)) {
        for (Pair<Attribute, Object> attr : nm.inputPortAttributes.get(input)) {
          dag.setInputPortAttribute(input, attr.getLeft(), attr.getValue());
        }
      }
    }

    // set operator attributes
    for (Pair<Attribute, Object> attr : nm.operatorAttributes) {
      dag.setAttribute(nm.operator, attr.getLeft(), attr.getValue());
    }

  }

  public NodeMeta addNode(String nodeName, Operator operator, NodeMeta parent, Operator.OutputPort parentOutput, Operator.InputPort inputPort)
  {

    NodeMeta newNode = new NodeMeta(operator, nodeName);
    if (parent == null) {
      heads.add(newNode);
    } else {
      parent.nodeStreams.get(parentOutput).getLeft().add(inputPort);
      parent.children.add(newNode);
      newNode.parent.add(parent);
    }
    return newNode;
  }

}
