/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.mroperator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import com.datatorrent.lib.io.fs.AbstractHDFSInputOperator;
import com.datatorrent.lib.util.KeyHashValPair;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.annotation.ShipContainingJars;

import com.datatorrent.demos.mroperator.ReporterImpl.ReporterType;

/**
 * <p>MapOperator class.</p>
 *
 * @since 0.9.0
 */
@ShipContainingJars(classes = { org.apache.hadoop.mapred.Reporter.class })
@SuppressWarnings({ "unchecked", "deprecation" })
public class MapOperator<K1, V1, K2, V2> extends AbstractHDFSInputOperator implements Partitioner<MapOperator<K1, V1, K2, V2>>
{

  private static final Logger logger = LoggerFactory.getLogger(MapOperator.class);

  private String dirName;

  private boolean emitPartitioningCountOnce = false;
  private boolean emitLastCountOnce = false;
  private int operatorId;
  private Class<? extends InputFormat<K1, V1>> inputFormatClass;
  private transient InputFormat<K1, V1> inputFormat;
  private transient InputSplit inputSplit;
  private Class<? extends InputSplit> inputSplitClass;
  private ByteArrayOutputStream outstream = new ByteArrayOutputStream();
  private transient RecordReader<K1, V1> reader;
  private boolean emittedAll = false;
  public final transient DefaultOutputPort<KeyHashValPair<Integer, Integer>> outputCount = new DefaultOutputPort<KeyHashValPair<Integer, Integer>>();
  public final transient DefaultOutputPort<KeyHashValPair<K2, V2>> output = new DefaultOutputPort<KeyHashValPair<K2, V2>>();
  private transient JobConf jobConf;

  public Class<? extends InputSplit> getInputSplitClass()
  {
    return inputSplitClass;
  }

  public void setInputSplitClass(Class<? extends InputSplit> inputSplitClass)
  {
    this.inputSplitClass = inputSplitClass;
  }

  public Class<? extends InputFormat<K1, V1>> getInputFormatClass()
  {
    return inputFormatClass;
  }

  public void setInputFormatClass(Class<? extends InputFormat<K1, V1>> inputFormatClass)
  {
    this.inputFormatClass = inputFormatClass;
  }

  public String getDirName()
  {
    return dirName;
  }

  public void setDirName(String dirName)
  {
    this.dirName = dirName;
    super.setFilePath(dirName);
  }

  @Override
  public void beginWindow(long windowId)
  {
    if (!emitPartitioningCountOnce) {
      outputCount.emit(new KeyHashValPair<Integer, Integer>(operatorId, 1));
      emitPartitioningCountOnce = true;
    }
    if (reader == null) {
      try {
        reader = inputFormat.getRecordReader(inputSplit, new JobConf(new Configuration()), reporter);
      } catch (IOException e) {
        logger.info("error getting record reader {}", e.getMessage());
      }
    }
    super.beginWindow(windowId);
  }

  @Override
  public void setup(OperatorContext context)
  {
    // logger.info("setup");
    if (context != null)
      operatorId = context.getId();
    reporter = new ReporterImpl(ReporterType.Mapper, new Counters());
    outputCollector = new OutputCollectorImpl<K2, V2>();
    Configuration conf = new Configuration();
    try {
      inputFormat = inputFormatClass.newInstance();
      SerializationFactory serializationFactory = new SerializationFactory(conf);
      Deserializer keyDesiralizer = serializationFactory.getDeserializer(inputSplitClass);
      keyDesiralizer.open(new ByteArrayInputStream(outstream.toByteArray()));
      inputSplit = (InputSplit) keyDesiralizer.deserialize(null);
      ((ReporterImpl) reporter).setInputSplit(inputSplit);
      reader = inputFormat.getRecordReader(inputSplit, new JobConf(conf), reporter);

    } catch (Exception e) {
      logger.info("failed to initialize inputformat obj {}", inputFormat);
    }
    InputStream stream = null;
    if (configFile != null && configFile.length() > 0) {
      // logger.info("system /{}", configFile);
      stream = ClassLoader.getSystemResourceAsStream("/" + configFile);

      if (stream == null) {
        // logger.info("system {}", configFile);
        stream = ClassLoader.getSystemResourceAsStream(configFile);

      }
    }
    if (stream != null) {
      // logger.info("dumping stream {}",getStringFromInputStream(stream));
      // logger.info("found our stream... so adding it");
      conf.addResource(stream);
    }
    jobConf = new JobConf(conf);
    if (mapClass != null) {
      try {
        mapObject = mapClass.newInstance();
      } catch (Exception e) {
        logger.info("can't instantiate object {}", e.getMessage());
      }

      mapObject.configure(jobConf);
    }
    if (combineClass != null) {
      try {
        combineObject = combineClass.newInstance();
      } catch (Exception e) {
        logger.info("can't instantiate object {}", e.getMessage());
      }
      combineObject.configure(jobConf);
    }
  }

  @Override
  public void activate(OperatorContext context)
  {
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  public void emitTuples(FSDataInputStream stream)
  {

    if (!emittedAll) {
      try {

        K1 key = reader.createKey();
        V1 val = reader.createValue();
        emittedAll = !reader.next(key, val);
        if (!emittedAll) {
          KeyHashValPair<K1, V1> keyValue = new KeyHashValPair<K1, V1>(key, val);
          mapObject.map(keyValue.getKey(), keyValue.getValue(), outputCollector, reporter);
          if (combineObject == null) {
            List<KeyHashValPair<K2, V2>> list = ((OutputCollectorImpl<K2, V2>) outputCollector).getList();
            for (KeyHashValPair<K2, V2> e : list) {
              output.emit(e);
            }
            list.clear();
          }
        }
      } catch (IOException ex) {
        logger.debug(ex.toString());
      }
    }
  }

  @Override
  public void endWindow()
  {

    List<KeyHashValPair<K2, V2>> list = ((OutputCollectorImpl<K2, V2>) outputCollector).getList();

    // logger.info("in end window {}",list);
    if (combineObject != null) {
      Map<K2, List<V2>> cacheObject = new HashMap<K2, List<V2>>();
      for (KeyHashValPair<K2, V2> tuple : list) {
        List<V2> cacheList = cacheObject.get(tuple.getKey());
        if (cacheList == null) {
          cacheList = new ArrayList<V2>();
          cacheList.add(tuple.getValue());
          cacheObject.put(tuple.getKey(), cacheList);
        } else {
          cacheList.add(tuple.getValue());
        }
      }
      list.clear();
      OutputCollector<K2, V2> tempOutputCollector = new OutputCollectorImpl<K2, V2>();
      for (Map.Entry<K2, List<V2>> e : cacheObject.entrySet()) {
        try {
          combineObject.reduce(e.getKey(), e.getValue().iterator(), tempOutputCollector, reporter);
        } catch (IOException e1) {
          logger.info(e1.getMessage());
        }
      }
      list = ((OutputCollectorImpl<K2, V2>) tempOutputCollector).getList();
      // logger.info("combiner output {}",list);
      for (KeyHashValPair<K2, V2> e : list) {
        // logger.info("combiner output {}",list.get(0));
        output.emit(e);
      }
    }

    if (!emitLastCountOnce && emittedAll) {
      outputCount.emit(new KeyHashValPair<Integer, Integer>(operatorId, -1));
      logger.info("emitting end of file {}", new KeyHashValPair<Integer, Integer>(operatorId, -1));
      emitLastCountOnce = true;
    }

    list.clear();
  }

  private InputSplit[] getSplits(JobConf conf, int numSplits, String path) throws IOException
  {
    FileInputFormat.setInputPaths(conf, new Path(path));
    if (inputFormat == null) {
      try {
        inputFormat = inputFormatClass.newInstance();
        String inputFormatClassName = inputFormatClass.getName();
        if (inputFormatClassName.equals("org.apache.hadoop.mapred.TextInputFormat")) {
          ((TextInputFormat) inputFormat).configure(conf);
        } else if (inputFormatClassName.equals("org.apache.hadoop.mapred.KeyValueTextInputFormat")) {
          ((KeyValueTextInputFormat) inputFormat).configure(conf);
        }

      } catch (InstantiationException e) {
      } catch (IllegalAccessException e) {
      }
    }
    return inputFormat.getSplits(conf, numSplits);
    // return null;
  }

  @Override
  public void partitioned(Map<Integer, Partition<MapOperator<K1, V1, K2, V2>>> partitions)
  {
  }

  @Override
  public Collection<Partition<MapOperator<K1, V1, K2, V2>>> definePartitions(Collection<Partition<MapOperator<K1, V1, K2, V2>>> partitions, int incrementalCapacity)
  {

    @SuppressWarnings("rawtypes")
    Collection c = partitions;
    Collection<Partition<MapOperator<K1, V1, K2, V2>>> operatorPartitions = c;
    Partition<MapOperator<K1, V1, K2, V2>> template = null;
    Iterator<Partition<MapOperator<K1, V1, K2, V2>>> itr = operatorPartitions.iterator();
    template = itr.next();
    Configuration conf = new Configuration();
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    // logger.info("inside partionfunction ");

    if (outstream.size() == 0) {
      InputSplit[] splits;
      // logger.info("creating splits");
      try {
        splits = getSplits(new JobConf(conf), incrementalCapacity + 1, template.getPartitionedInstance().getDirName());
      } catch (IOException e1) {
        logger.info(" can't get splits {}", e1.getMessage());
        return null;
      }
      // logger.info("created splits");

      Collection<Partition<MapOperator<K1, V1, K2, V2>>> operList = new ArrayList<Partition<MapOperator<K1, V1, K2, V2>>>();

      itr = operatorPartitions.iterator();
      int size = splits.length;
      Serializer keySerializer = serializationFactory.getSerializer(splits[0].getClass());

      // logger.info(" size of splits {}", size);
      while (size > 0 && itr.hasNext()) {
        Partition<MapOperator<K1, V1, K2, V2>> p = itr.next();
        MapOperator<K1, V1, K2, V2> opr = p.getPartitionedInstance();
        opr.setInputFormatClass(inputFormatClass);
        opr.setMapClass(mapClass);
        opr.setCombineClass(combineClass);
        opr.setConfigFile(configFile);
        // logger.info(" size of splits {}", size);
        try {
          keySerializer.open(opr.getOutstream());
          keySerializer.serialize(splits[size - 1]);
          opr.setInputSplitClass(splits[size - 1].getClass());
        } catch (IOException e) {
          logger.info("error while serializing {}", e.getMessage());
        }
        size--;
        operList.add(p);
      }
      while (size > 0) {
        // logger.info(" size of splits {}", size);
        MapOperator<K1, V1, K2, V2> opr = new MapOperator<K1, V1, K2, V2>();
        opr.setInputFormatClass(inputFormatClass);
        opr.setMapClass(mapClass);
        opr.setCombineClass(combineClass);
        opr.setConfigFile(configFile);
        try {
          keySerializer.open(opr.getOutstream());
          keySerializer.serialize(splits[size - 1]);
          opr.setInputSplitClass(splits[size - 1].getClass());
        } catch (IOException e) {
          logger.info("error while serializing {}", e.getMessage());
        }

        size--;
        operList.add(new DefaultPartition<MapOperator<K1, V1, K2, V2>>(opr));
      }
      try {
        keySerializer.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

      return operList;
    }

    return null;

  }

  public ByteArrayOutputStream getOutstream()
  {
    return outstream;
  }

  public void setOutstream(ByteArrayOutputStream outstream)
  {
    this.outstream = outstream;
  }

  /**
   * adding map code
   */

  private Class<? extends Mapper<K1, V1, K2, V2>> mapClass;
  private Class<? extends Reducer<K2, V2, K2, V2>> combineClass;

  private transient Mapper<K1, V1, K2, V2> mapObject;
  private transient Reducer<K2, V2, K2, V2> combineObject;
  private transient Reporter reporter;

  private String configFile;

  public String getConfigFile()
  {
    return configFile;
  }

  public void setConfigFile(String configFile)
  {
    this.configFile = configFile;
  }

  private transient OutputCollector<K2, V2> outputCollector;

  public Class<? extends Mapper<K1, V1, K2, V2>> getMapClass()
  {
    return mapClass;
  }

  public void setMapClass(Class<? extends Mapper<K1, V1, K2, V2>> mapClass)
  {
    this.mapClass = mapClass;
  }

  public Class<? extends Reducer<K2, V2, K2, V2>> getCombineClass()
  {
    return combineClass;
  }

  public void setCombineClass(Class<? extends Reducer<K2, V2, K2, V2>> combineClass)
  {
    this.combineClass = combineClass;
  }

}
