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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.apex.malhar.lib.window.ControlTuple;
import org.apache.apex.malhar.lib.window.impl.AbstractWindowedOperator;
import org.apache.apex.malhar.stream.api.Option;
import org.apache.apex.malhar.stream.api.annotation.ControlPort;
import org.apache.apex.malhar.stream.api.operator.PostprocessOperator;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * Graph data structure for DAG
 * With this data structure, the framework can do lazy load and optimization
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public class DagMeta
{

  public static String POST_PROCESS_OPERATOR_SUFFIX = "_PostProcess";

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

    public Option[] getOptions()
    {
      return options;
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
      visitNode(nm, dag, null);
    }
  }

  private void visitNode(NodeMeta nm, DAG dag, DefaultOutputPort<ControlTuple> controlTupleOutputPort)
  {

    DefaultOutputPort<ControlTuple> possibleControlTuplePort = controlTupleOutputPort;
    // add logical operator
    dag.addOperator(nm.getOperatorName(), nm.operator);

    possibleControlTuplePort = connectControlPort(dag, possibleControlTuplePort, nm.getOperator());

    PostprocessOperator postprocessOperator = null;
    //deal with options
    for (Option opt : nm.getOptions()) {
      if (opt instanceof Option.WatermarkGenerator) {
        postprocessOperator = getOrNew(postprocessOperator);
        postprocessOperator.setWatermarkGenerator((Option.WatermarkGenerator)opt);
      }
    }

    if (postprocessOperator != null) {
      dag.addOperator(nm.getOperatorName() + POST_PROCESS_OPERATOR_SUFFIX, postprocessOperator);
      possibleControlTuplePort = connectControlPort(dag, possibleControlTuplePort, postprocessOperator);
      Map.Entry<Operator.OutputPort, Pair<List<Operator.InputPort>, DAG.Locality>> toReplace = null;
      for (Map.Entry<Operator.OutputPort, Pair<List<Operator.InputPort>, DAG.Locality>> entry : nm.nodeStreams.entrySet()) {
        if (entry.getKey() == null || entry.getValue().getKey() == null || 0 == entry.getValue().getKey().size()) {
          continue;
        } else {
          DAG.StreamMeta streamMeta = dag.addStream("inner_stream_post", entry.getKey(),
              postprocessOperator.dataInput);
          // always run postprocess operator to thread local
          streamMeta.setLocality(DAG.Locality.THREAD_LOCAL);
          toReplace = entry;
        }
      }
      if (toReplace != null) {
        Pair<List<Operator.InputPort>, DAG.Locality> val = nm.nodeStreams.remove(toReplace.getKey());
        nm.nodeStreams.put(postprocessOperator.dataOutput, val);
      }
    }


    for (NodeMeta child : nm.children) {
      visitNode(child, dag, possibleControlTuplePort);
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

  /**
   * Connect transitiveControlTupleOutputPort to operator if the operator has control tuple port
   * and return the new control output port if possible
   * @param dag
   * @param transitiveControlTupleOutputPort
   * @param operator
   * @return
   */
  private DefaultOutputPort<ControlTuple> connectControlPort(DAG dag, DefaultOutputPort<ControlTuple> transitiveControlTupleOutputPort, Operator operator)
  {
    if (operator == null) {
      return transitiveControlTupleOutputPort;
    }

    DefaultOutputPort<ControlTuple> newOutput = null;
    DefaultInputPort<ControlTuple> inputToBeConnected = null;

    if (operator instanceof AbstractWindowedOperator) {
      inputToBeConnected = ((AbstractWindowedOperator)operator).controlInput;
      newOutput = ((AbstractWindowedOperator)operator).controlOutput;
    } else {
      Operator.Port[] inputOutput = findControlTuplePort(operator);
      inputToBeConnected = (DefaultInputPort<ControlTuple>)inputOutput[0];
      newOutput = (DefaultOutputPort<ControlTuple>)inputOutput[1];
    }


    // connect control tuple port if possible
    if (transitiveControlTupleOutputPort != null && inputToBeConnected != null) {
      dag.addStream(IDGenerator.generateControlStreamNameWithUUID(), transitiveControlTupleOutputPort, inputToBeConnected);
    }

    return newOutput != null ? newOutput : transitiveControlTupleOutputPort;
  }

  private Operator.Port[] findControlTuplePort(Operator operator)
  {
    Operator.Port[] result = new Operator.Port[2];
    try {
      for (Field f : operator.getClass().getFields()) {
        for (Annotation an : f.getDeclaredAnnotations()) {
          if (an instanceof ControlPort) {
            if (Operator.InputPort.class.isAssignableFrom(f.getType())) {
              result[0] = (Operator.Port)f.get(operator);
            } else if (Operator.OutputPort.class.isAssignableFrom(f.getType())) {
              result[1] = (Operator.Port)f.get(operator);
            }
          }
        }
      }
    } catch (IllegalAccessException e) {
      // should never get into this block
      throw new RuntimeException("Port field needs to be public");
    }
    return result;
  }

  private PostprocessOperator getOrNew(PostprocessOperator postprocessOperator)
  {
    if (postprocessOperator == null) {
      return new PostprocessOperator();
    } else {
      return postprocessOperator;
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

}
