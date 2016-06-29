package org.apache.apex.malhar.stream.api.impl;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.joda.time.Duration;

import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.function.Function;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;

import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;

import com.datatorrent.lib.util.KeyValPair;

/**
 * Created by siyuan on 6/22/16.
 */
public class ApexWindowedStreamImpl<T> extends ApexStreamImpl<T> implements WindowedStream<T>
{

  public static class Count implements Accumulation<Long, Long, Long>
  {
    @Override
    public Long defaultAccumulatedValue()
    {
      return 0L;
    }

    @Override
    public Long accumulate(Long accumulatedValue, Long input)
    {
      return accumulatedValue + input;
    }

    @Override
    public Long merge(Long accumulatedValue1, Long accumulatedValue2)
    {
      return accumulatedValue1 + accumulatedValue2;
    }

    @Override
    public Long getOutput(Long accumulatedValue)
    {
      return accumulatedValue;
    }

    @Override
    public Long getRetraction(Long accumulatedValue)
    {
      return -accumulatedValue;
    }
  }


  public static class TopN<T> implements Accumulation<T, List<T>, List<T>>
  {

    int n;

    Comparator<T> comparator;

    public void setN(int n)
    {
      this.n = n;
    }

    public void setComparator(Comparator<T> comparator)
    {
      this.comparator = comparator;
    }

    @Override
    public List<T> defaultAccumulatedValue()
    {
      return new LinkedList<>();
    }

    @Override
    public List<T> accumulate(List<T> accumulatedValue, T input)
    {
      int k = 0;
      for (T inMemory : accumulatedValue) {
        if (comparator!=null) {
          if (comparator.compare(inMemory, input) < 0)
            break;
        } else if (input instanceof Comparable) {
          if (((Comparable<T>)input).compareTo(inMemory) > 0)
            break;
        } else {
          throw new RuntimeException("Tuple cannot be compared");
        }
        k++;
      }
      accumulatedValue.add(k, input);
      if (accumulatedValue.size() > n) {
        accumulatedValue.remove(accumulatedValue.get(accumulatedValue.size() - 1));
      }
      return accumulatedValue;
    }

    @Override
    public List<T> merge(List<T> accumulatedValue1, List<T> accumulatedValue2)
    {
      accumulatedValue1.addAll(accumulatedValue2);
      if (comparator != null) {
        Collections.sort(accumulatedValue1, Collections.reverseOrder(comparator));
      } else {
        Collections.sort(accumulatedValue1, Collections.reverseOrder());
      }
      if (accumulatedValue1.size() > n) {
        return accumulatedValue1.subList(0, n);
      } else {
        return accumulatedValue1;
      }
    }

    @Override
    public List<T> getOutput(List<T> accumulatedValue)
    {
      return accumulatedValue;
    }

    @Override
    public List<T> getRetraction(List<T> accumulatedValue)
    {
      return new LinkedList<>();
    }
  }

  protected WindowOption windowOption;

  protected TriggerOption triggerOption;

  protected Duration allowedLateness;


  @Override
  public <STREAM extends WindowedStream<Long>> STREAM count()
  {
    return null;
  }

  @Override
  public <K, STREAM extends WindowedStream<Tuple<KeyValPair<K, Long>>>> STREAM countByKey(Function.MapFunction<T, Tuple<KeyValPair<K, Long>>> convertToKeyValue)
  {
    WindowedStream<Tuple<KeyValPair<K, Long>>> kvstream = map(convertToKeyValue);
    KeyedWindowedOperatorImpl<K, Long, Long, Long> keyedWindowedOperator = new KeyedWindowedOperatorImpl<>();

    //TODO use other default setting in the future
    keyedWindowedOperator.setDataStorage(new InMemoryWindowedKeyedStorage<K, Long>());
    keyedWindowedOperator.setRetractionStorage(new InMemoryWindowedKeyedStorage<K, Long>());
    keyedWindowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    if (windowOption != null)
      keyedWindowedOperator.setWindowOption(windowOption);
    if (triggerOption != null)
      keyedWindowedOperator.setTriggerOption(triggerOption);
    if (allowedLateness != null)
      keyedWindowedOperator.setAllowedLateness(allowedLateness);
    keyedWindowedOperator.setAccumulation(new Count());
    return kvstream.addOperator(keyedWindowedOperator, keyedWindowedOperator.input, keyedWindowedOperator.output);

  }

  @Override
  public <STREAM extends WindowedStream<Map<Object, Integer>>> STREAM countByKey(int key)
  {
    return null;
  }

  @Override
  public <V, K, STREAM extends WindowedStream<Tuple<KeyValPair<K, List<V>>>>> STREAM topByKey(int N, Function.MapFunction<T, Tuple<KeyValPair<K, V>>> convertToKeyVal)
  {
    WindowedStream<Tuple<KeyValPair<K, V>>> kvstream = map(convertToKeyVal);
    KeyedWindowedOperatorImpl<K, V, List<V>, List<V>> keyedWindowedOperator = new KeyedWindowedOperatorImpl<>();

    //TODO use other default setting in the future
    keyedWindowedOperator.setDataStorage(new InMemoryWindowedKeyedStorage<K, List<V>>());
    keyedWindowedOperator.setRetractionStorage(new InMemoryWindowedKeyedStorage<K, List<V>>());
    keyedWindowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    if (windowOption != null)
      keyedWindowedOperator.setWindowOption(windowOption);
    if (triggerOption != null)
      keyedWindowedOperator.setTriggerOption(triggerOption);
    if (allowedLateness != null)
      keyedWindowedOperator.setAllowedLateness(allowedLateness);
    TopN<V> top = new TopN<>();
    top.setN(N);
    keyedWindowedOperator.setAccumulation(top);
    return kvstream.addOperator(keyedWindowedOperator, keyedWindowedOperator.input, keyedWindowedOperator.output);
  }

  @Override
  public <STREAM extends WindowedStream<T>> STREAM top(int N)
  {
    return null;
  }

  @Override
  public <O, STREAM extends WindowedStream<O>> STREAM combineByKey()
  {
    return null;
  }

  @Override
  public <O, STREAM extends WindowedStream<O>> STREAM combine()
  {
    return null;
  }

  @Override
  public <STREAM extends WindowedStream<T>> STREAM reduce(String name, Function.ReduceFunction<T> reduce)
  {
    return null;
  }

  @Override
  public <O, STREAM extends WindowedStream<O>> STREAM fold(O initialValue, Function.FoldFunction<T, O> fold)
  {
    return null;
  }

  @Override
  public <O, STREAM extends WindowedStream<O>> STREAM fold(String name, O initialValue, Function.FoldFunction<T, O>
      fold)
  {
    return null;
  }

  @Override
  public <O, K, STREAM extends WindowedStream<KeyValPair<K, O>>> STREAM foldByKey(String name, Function.FoldFunction<T, KeyValPair<K, O>> fold)
  {
    return null;
  }

  @Override
  public <O, K, STREAM extends WindowedStream<KeyValPair<K, O>>> STREAM foldByKey(Function.FoldFunction<T,
      KeyValPair<K, O>> fold)
  {
    return null;
  }

  @Override
  public <STREAM extends WindowedStream<T>> STREAM reduce(Function.ReduceFunction<T> reduce)
  {
    return null;
  }

  @Override
  public <O, K, STREAM extends WindowedStream<KeyValPair<K, Iterable<O>>>> STREAM groupByKey(Function.MapFunction<T,
      KeyValPair<K, O>> convertToKeyVal)
  {
    return null;
  }

  @Override
  public <STREAM extends WindowedStream<Iterable<T>>> STREAM group()
  {
    return null;
  }

  @Override
  public <STREAM extends WindowedStream<T>> STREAM resetTrigger(TriggerOption option)
  {
    triggerOption = option;
    return (STREAM)this;
  }

  @Override
  public <STREAM extends WindowedStream<T>> STREAM resetAllowedLateness(Duration allowedLateness)
  {
    this.allowedLateness = allowedLateness;
    return (STREAM)this;
  }

  @Override
  protected <O> ApexStream<O> newStream(DagMeta graph, Brick<O> newBrick)
  {
    ApexWindowedStreamImpl<O> newstream = new ApexWindowedStreamImpl<>();
    newstream.graph = graph;
    newstream.lastBrick = newBrick;
    return newstream;
  }
}
