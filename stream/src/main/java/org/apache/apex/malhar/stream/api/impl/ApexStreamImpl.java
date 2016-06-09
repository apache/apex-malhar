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
import java.util.concurrent.Callable;

import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.CompositeStreamTransform;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.function.Function.FlatMapFunction;
import org.apache.apex.malhar.stream.api.operator.FunctionOperator;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.window.WindowOption;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * Default stream implementation for ApexStream interface.
 * It creates the dag(execution plan) from stream api
 *
 * @since 3.4.0
 */
public class ApexStreamImpl<T> implements ApexStream<T>, WindowedStream<T>
{

  private static Set<Attribute<?>> OPERATOR_ATTRIBUTES;

  private static Set<Attribute<?>> DAG_ATTRIBUTES;

  private static Set<Attribute<?>> INPUT_ATTRIBUTES;

  private static Set<Attribute<?>> OUTPUT_ATTRIBUTES;

  static {

    OPERATOR_ATTRIBUTES = new HashSet<>();
    DAG_ATTRIBUTES = new HashSet<>();
    INPUT_ATTRIBUTES = new HashSet<>();
    OUTPUT_ATTRIBUTES = new HashSet<>();

    try {
      for (Field field : Context.OperatorContext.class.getDeclaredFields()) {
        if (field.getType() == Attribute.class) {
          OPERATOR_ATTRIBUTES.add((Attribute)field.get(Context.OperatorContext.class));
        }
      }

      for (Field field : Context.DAGContext.class.getDeclaredFields()) {
        if (field.getType() == Attribute.class) {
          DAG_ATTRIBUTES.add((Attribute)field.get(Context.DAGContext.class));
        }
      }
    } catch (IllegalAccessException e) {
      //Ignore here
    }

    INPUT_ATTRIBUTES.add(Context.PortContext.PARTITION_PARALLEL);
    INPUT_ATTRIBUTES.add(Context.PortContext.AUTO_RECORD);
    INPUT_ATTRIBUTES.add(Context.PortContext.STREAM_CODEC);
    INPUT_ATTRIBUTES.add(Context.PortContext.TUPLE_CLASS);


    OUTPUT_ATTRIBUTES.add(Context.PortContext.QUEUE_CAPACITY);
    OUTPUT_ATTRIBUTES.add(Context.PortContext.BUFFER_MEMORY_MB);
    OUTPUT_ATTRIBUTES.add(Context.PortContext.SPIN_MILLIS);
    OUTPUT_ATTRIBUTES.add(Context.PortContext.UNIFIER_SINGLE_FINAL);
    OUTPUT_ATTRIBUTES.add(Context.PortContext.IS_OUTPUT_UNIFIED);
    OUTPUT_ATTRIBUTES.add(Context.PortContext.AUTO_RECORD);
    OUTPUT_ATTRIBUTES.add(Context.PortContext.STREAM_CODEC);
    OUTPUT_ATTRIBUTES.add(Context.PortContext.TUPLE_CLASS);

  }

  /**
   * The extension point of the stream
   *
   * @param <T>
   */
  public static class Brick<T>
  {

    private Operator.OutputPort<T> lastOutput;

    private DagMeta.NodeMeta nodeMeta;

    private Pair<Operator.OutputPort, Operator.InputPort> lastStream;

    public Operator.OutputPort<T> getLastOutput()
    {
      return lastOutput;
    }

    public void setLastOutput(Operator.OutputPort<T> lastOutput)
    {
      this.lastOutput = lastOutput;
    }

    public void setLastStream(Pair<Operator.OutputPort, Operator.InputPort> lastStream)
    {
      this.lastStream = lastStream;
    }

    public Pair<Operator.OutputPort, Operator.InputPort> getLastStream()
    {
      return lastStream;
    }
  }

  /**
   * Graph behind the stream
   */
  private DagMeta graph;

  private ApexStream<T> delegator;

  /**
   * Right now the stream only support single extend point
   * You can have multiple downstream operators connect to this single extend point though
   */
  private Brick<T> lastBrick;

  public Brick<T> getLastBrick()
  {
    return lastBrick;
  }

  public void setLastBrick(Brick<T> lastBrick)
  {
    this.lastBrick = lastBrick;
  }

  public ApexStreamImpl()
  {
    graph = new DagMeta();
  }

  public ApexStreamImpl(ApexStream<T> apexStream)
  {
    this.delegator = apexStream;
    if (delegator != null && delegator instanceof ApexStreamImpl) {
      graph = ((ApexStreamImpl)delegator).graph;
      lastBrick = ((ApexStreamImpl<T>)delegator).lastBrick;
    }
  }

  public ApexStreamImpl(DagMeta graph)
  {
    this(graph, null);
  }

  public ApexStreamImpl(DagMeta graph, Brick<T> lastBrick)
  {
    this.graph = graph;
    this.lastBrick = lastBrick;
  }

  @Override
  public <O, STREAM extends ApexStream<O>> STREAM map(Function.MapFunction<T, O> mf)
  {
    return map(mf.toString(), mf);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <O, STREAM extends ApexStream<O>> STREAM map(String name, Function.MapFunction<T, O> mf)
  {
    FunctionOperator.MapFunctionOperator<T, O> opt = new FunctionOperator.MapFunctionOperator<>(mf);
    return (STREAM)addOperator(name, opt, opt.input, opt.output);
  }

  @Override
  public <O, STREAM extends ApexStream<O>> STREAM flatMap(FlatMapFunction<T, O> flatten)
  {
    return flatMap(flatten.toString(), flatten);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <O, STREAM extends ApexStream<O>> STREAM flatMap(String name, FlatMapFunction<T, O> flatten)
  {
    FunctionOperator.FlatMapFunctionOperator<T, O> opt = new FunctionOperator.FlatMapFunctionOperator<>(flatten);
    return (STREAM)addOperator(name, opt, opt.input, opt.output);
  }

  @Override
  public <STREAM extends ApexStream<T>> STREAM filter(final Function.FilterFunction<T> filter)
  {
    return filter(filter.toString(), filter);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <STREAM extends ApexStream<T>> STREAM filter(String name, final Function.FilterFunction<T> filter)
  {
    FunctionOperator.FilterFunctionOperator<T> filterFunctionOperator = new FunctionOperator.FilterFunctionOperator<>(filter);
    return (STREAM)addOperator(name, filterFunctionOperator, filterFunctionOperator.input, filterFunctionOperator.output);
  }

  @Override
  public <STREAM extends ApexStream<T>> STREAM reduce(Function.ReduceFunction<T> reduce)
  {
    return reduce(reduce.toString(), reduce);
  }

  @Override
  public <O, K, STREAM extends ApexStream<KeyValPair<K, Iterable<O>>>> STREAM groupByKey()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <STREAM extends ApexStream<Iterable<T>>> STREAM group()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <STREAM extends ApexStream<T>> STREAM reduce(String name, Function.ReduceFunction<T> reduce)
  {
    FunctionOperator.ReduceFunctionOperator<T> opt = new FunctionOperator.ReduceFunctionOperator<>(reduce);
    return (STREAM)addOperator(name, opt, opt.input, opt.output);
  }

  @Override
  public <O, STREAM extends ApexStream<O>> STREAM fold(final O initialValue, Function.FoldFunction<T, O> fold)
  {
    return fold(fold.toString(), initialValue, fold);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <O, STREAM extends ApexStream<O>> STREAM fold(String name, O initialValue, Function.FoldFunction<T, O> fold)
  {
    FunctionOperator.FoldFunctionOperator<T, O> opt = new FunctionOperator.FoldFunctionOperator<>(fold, initialValue);
    return (STREAM)addOperator(name, opt, opt.input, opt.output);
  }

  @Override
  public <O, K, STREAM extends ApexStream<KeyValPair<K, O>>> STREAM foldByKey(String name, Function.FoldFunction<T, KeyValPair<K, O>> fold)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <O, K, STREAM extends ApexStream<KeyValPair<K, O>>> STREAM foldByKey(Function.FoldFunction<T, KeyValPair<K, O>> fold)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <STREAM extends ApexStream<Integer>> STREAM count()
  {
    throw new UnsupportedOperationException();
  }


  public <STREAM extends ApexStream<Map.Entry<Object, Integer>>> STREAM countByElement()
  {
    return null;
  }

  @Override
  public <STREAM extends ApexStream<Map<Object, Integer>>> STREAM countByKey(int key)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <TUPLE, KEY, STREAM extends ApexStream<Map.Entry<KEY, List<TUPLE>>>> STREAM topByKey(int N)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <STREAM extends ApexStream<T>> STREAM top(int N)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <O, STREAM extends ApexStream<O>> STREAM combineByKey()
  {
    return null;
  }

  @Override
  public <O, STREAM extends ApexStream<O>> STREAM combine()
  {
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <STREAM extends ApexStream<Map.Entry<Object, Integer>>> STREAM countByKey()
  {
    // Needs to change the unique counter to support keys
    UniqueCounter<Object> uniqueCounter = new UniqueCounter<>();
    uniqueCounter.setCumulative(true);
    Operator.OutputPort<HashMap<Object, Integer>> resultPort = uniqueCounter.count;
    return (STREAM)addOperator("CounterByKey", uniqueCounter, (Operator.InputPort<T>)uniqueCounter.data, resultPort)
        .flatMap(new FlatMapFunction<HashMap<Object, Integer>, Map.Entry<Object, Integer>>()
        {
          @Override
          public Iterable<Map.Entry<Object, Integer>> f(HashMap<Object, Integer> input)
          {
            return input.entrySet();
          }
        });
  }

  @Override
  public <O, STREAM extends ApexStream<O>> STREAM addOperator(Operator op, Operator.InputPort<T> inputPort, Operator.OutputPort<O> outputPort)
  {
    return addOperator(op.toString(), op, inputPort, outputPort);
  }


  @Override
  @SuppressWarnings("unchecked")
  public <O, STREAM extends ApexStream<O>> STREAM addOperator(String opName, Operator op, Operator.InputPort<T> inputPort, Operator.OutputPort<O> outputPort)
  {

    if (delegator != null) {
      ApexStreamImpl<O> apexStream = delegator.addOperator(opName, op, inputPort, outputPort);
      try {
        return (STREAM)this.getClass().getConstructor(ApexStream.class).newInstance(apexStream);
      } catch (Exception e) {
        throw new RuntimeException("You have to override the default constructor with ApexStreamImpl as delegator");
      }
    }

    checkArguments(op, inputPort, outputPort);

    DagMeta.NodeMeta nm = null;

    if (lastBrick == null) {
      nm = graph.addNode(opName, op, null, null, inputPort);
    } else {

      nm = graph.addNode(opName, op, lastBrick.nodeMeta, lastBrick.lastOutput, inputPort);
    }

    Brick<O> newBrick = new Brick<>();
    newBrick.nodeMeta = nm;
    newBrick.setLastOutput(outputPort);
    if (lastBrick != null) {
      newBrick.lastStream = Pair.<Operator.OutputPort, Operator.InputPort>of(lastBrick.lastOutput, inputPort);
    }

    return (STREAM)new ApexStreamImpl<>(this.graph, newBrick);
  }

  @Override
  public <O, STREAM extends ApexStream<O>> STREAM addCompositeStreams(CompositeStreamTransform<T, O> compositeStreamTransform)
  {
    throw new UnsupportedOperationException();
  }

  /* Check to see if inputPort and outputPort belongs to the operator */
  private void checkArguments(Operator op, Operator.InputPort inputPort, Operator.OutputPort outputPort)
  {
    if (op == null) {
      throw new IllegalArgumentException("Operator can not be null");
    }

    boolean foundInput = inputPort == null;
    boolean foundOutput = outputPort == null;
    for (Field f : op.getClass().getFields()) {
      int modifiers = f.getModifiers();
      if (!Modifier.isPublic(modifiers) || !Modifier.isTransient(modifiers)) {
        continue;
      }
      Object obj = null;
      try {
        obj = f.get(op);
      } catch (IllegalAccessException e) {
        // NonAccessible field is not a valid port object
      }
      if (obj == outputPort) {
        foundOutput = true;
      }
      if (obj == inputPort) {
        foundInput = true;
      }
    }
    if (!foundInput || !foundOutput) {
      throw new IllegalArgumentException("Input port " + inputPort + " and/or Output port " + outputPort + " is/are not owned by Operator " + op);
    }

  }

  @Override
  public <STREAM extends ApexStream<T>> STREAM union(ApexStream<T>... others)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public ApexStreamImpl<T> print()
  {
    ConsoleOutputOperator consoleOutputOperator = new ConsoleOutputOperator();
    addOperator(IDGenerator.generateOperatorIDWithUUID(consoleOutputOperator.getClass()), consoleOutputOperator,
        (Operator.InputPort<T>)consoleOutputOperator.input, null);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public ApexStream<T> printErr()
  {
    //TODO need to make ConsoleOutputOperator support stderr
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public ApexStream<T> with(Attribute attribute, Object value)
  {
    if (OPERATOR_ATTRIBUTES.contains(attribute)) {
      lastBrick.nodeMeta.operatorAttributes.add(Pair.of(attribute, value));
    }

    if (INPUT_ATTRIBUTES.contains(attribute)) {
      if (lastBrick.lastStream != null) {
        List<Pair<Attribute, Object>> attrs = lastBrick.nodeMeta.inputPortAttributes.get(lastBrick.lastStream.getRight());
        if (attrs == null) {
          attrs = new LinkedList<>();
        }
        attrs.add(Pair.of(attribute, value));
        lastBrick.nodeMeta.inputPortAttributes.put(lastBrick.lastStream.getRight(), attrs);
      }
    }

    if (OUTPUT_ATTRIBUTES.contains(attribute)) {
      if (lastBrick.lastStream != null) {

        for (DagMeta.NodeMeta parent : lastBrick.nodeMeta.getParent()) {
          parent.getNodeStreams().containsKey(lastBrick.lastStream.getLeft());
          List<Pair<Attribute, Object>> attrs = parent.outputPortAttributes.get(lastBrick.lastStream.getLeft());
          if (attrs == null) {
            attrs = new LinkedList<>();
          }
          attrs.add(Pair.of(attribute, value));
          lastBrick.nodeMeta.outputPortAttributes.put(lastBrick.lastStream.getLeft(), attrs);
        }
      }
    }

    setGlobalAttribute(attribute, value);

    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public ApexStream<T> setGlobalAttribute(Attribute attribute, Object value)
  {
    graph.dagAttributes.add(Pair.of(attribute, value));
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public ApexStream<T> with(DAG.Locality locality)
  {
    if (lastBrick.lastStream != null) {
      for (DagMeta.NodeMeta parent : lastBrick.nodeMeta.getParent()) {
        Pair<List<Operator.InputPort>, DAG.Locality> p = parent.getNodeStreams().get(lastBrick.lastStream.getLeft());
        if (p != null) {
          p.setValue(locality);
        }
      }
    }
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public ApexStream<T> with(String propName, Object value)
  {
    try {
      BeanUtils.setProperty(lastBrick.nodeMeta.getOperator(), propName, value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return this;
  }


  @Override
  public DAG createDag()
  {
    LogicalPlan dag = new LogicalPlan();
    populateDag(dag);
    return dag;
  }

  @Override
  public void populateDag(DAG dag)
  {
    graph.buildDAG(dag);
  }

  @Override
  public void runEmbedded(boolean async, long duration, Callable<Boolean> exitCondition)
  {
    LocalMode lma = LocalMode.newInstance();
    populateDag(lma.getDAG());
    LocalMode.Controller lc = lma.getController();
    if (lc instanceof StramLocalCluster) {
      ((StramLocalCluster)lc).setExitCondition(exitCondition);
    }
    if (async) {
      lc.runAsync();
    } else {
      if (duration >= 0) {
        lc.run(duration);
      } else {
        lc.run();
      }
    }

  }


  @Override
  public void run()
  {
    throw new UnsupportedOperationException();
    //TODO need an api to submit the StreamingApplication to cluster
  }

  @Override
  public WindowedStream<T> window(WindowOption option)
  {
    return null;
  }

}
