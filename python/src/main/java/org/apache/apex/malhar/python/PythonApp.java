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
package org.apache.apex.malhar.python;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.PythonConstants;
import org.apache.apex.malhar.kafka.PartitionStrategy;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;

import org.apache.apex.malhar.python.operator.PythonGenericOperator;
import org.apache.apex.malhar.python.operator.proxy.PythonReduceProxy;
import org.apache.apex.malhar.python.runtime.PythonApexStreamImpl;
import org.apache.apex.malhar.python.runtime.PythonWorkerContext;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.Option;
import org.apache.apex.malhar.stream.api.PythonApexStream;
import org.apache.apex.malhar.stream.api.impl.ApexStreamImpl;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.stram.client.StramAppLauncher;

public class PythonApp implements StreamingApplication
{

  private PythonApexStream apexStream = null;
  private StreamFactory streamFactory;
  private ApplicationId appId = null;
  private static final Logger LOG = LoggerFactory.getLogger(PythonApp.class);

  private PythonAppManager manager = null;
  private String name;
  private Configuration conf;

  private String apexDirectoryPath = null;
  private PythonAppManager.LaunchMode mode = PythonAppManager.LaunchMode.HADOOP;

  public PythonApp()
  {
    this(null, null);

  }

  public PythonApp(String name)
  {

    this(name, null);
  }

  public PythonApp(String name, ApplicationId appId)
  {
    this.appId = appId;
    this.name = name;
    this.conf = new Configuration(true);
    this.apexDirectoryPath = System.getenv("PYAPEX_HOME");
    this.conf.set("dt.loggers.level", "com.datatorrent.*:INFO,org.apache.*:DEBUG");
  }

  public String getApexDirectoryPath()
  {
    return apexDirectoryPath;
  }

  public String getName()
  {
    return name;
  }

  public void populateDAG(DAG dag, Configuration conf)
  {

    LOG.trace("Populating DAG in python app");
    this.apexStream.populateDag(dag);

  }

  public void setRequiredJARFiles()
  {

    LOG.debug("PYAPEX_HOME: {}" + getApexDirectoryPath());
    File dir = new File(this.getApexDirectoryPath() + "/deps/");
    File[] files = dir.listFiles();
    ArrayList<String> jarFiles = new ArrayList<String>();
    for (File jarFile : files) {
      LOG.info("FOUND FILES {}" + jarFile.getAbsolutePath());
      jarFiles.add(jarFile.getAbsolutePath());

    }
    jarFiles.add(this.getApexDirectoryPath() + "/deps/" + PythonConstants.PY4J_SRC_ZIP_FILE_NAME);
    jarFiles.add(this.getApexDirectoryPath() + "/src/pyapex/runtime/" + PythonConstants.PYTHON_WORKER_FILE_NAME);
    jarFiles.add(this.getApexDirectoryPath() + "/deps/" + PythonConstants.PYTHON_APEX_ZIP_NAME);

    extendExistingConfig(StramAppLauncher.LIBJARS_CONF_KEY_NAME, jarFiles);
//    this.getClassPaths();
  }

  public List<String> getClassPaths()
  {
    LOG.info("PROCESSING CLASSPATH");
    List<String> paths = new ArrayList<>();
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    URL[] urls = ((URLClassLoader)cl).getURLs();
    for (URL url : urls) {
      LOG.info("FOUND FILE PATH {}" + url.getFile());
      paths.add(url.getFile());
    }
    return paths;
  }

  public void setRequiredRuntimeFiles()
  {

    ArrayList<String> files = new ArrayList<String>();
    files.add(this.getApexDirectoryPath() + "/deps/" + PythonConstants.PY4J_SRC_ZIP_FILE_NAME);
    files.add(this.getApexDirectoryPath() + "/src/pyapex/runtime/" + PythonConstants.PYTHON_WORKER_FILE_NAME);
    files.add(this.getApexDirectoryPath() + "/deps/" + PythonConstants.PYTHON_APEX_ZIP_NAME);

    extendExistingConfig(StramAppLauncher.FILES_CONF_KEY_NAME, files);

  }

  public void extendExistingConfig(String fileVariable, ArrayList<String> fileList)
  {
    Configuration configuration = this.getConf();
    String fileCSV = configuration.get(fileVariable);
    String filesCSVToAppend = StringUtils.join(fileList, ",");

    if (StringUtils.isEmpty(fileCSV)) {
      fileCSV = filesCSVToAppend;

    } else {
      fileCSV = fileCSV + "," + filesCSVToAppend;
    }

    configuration.set(fileVariable, fileCSV);
  }

  public Configuration getConf()
  {
    return conf;
  }

  public void setConf(Configuration conf)
  {
    this.conf = conf;
  }

  public StreamFactory getStreamFactory()
  {
    if (streamFactory == null) {
      streamFactory = new StreamFactory();
    }
    return streamFactory;
  }

  public String launch(boolean local) throws Exception
  {

    LOG.debug("Already set Launching mode option : {} Local {} ", mode, local);

    if (local) {
      mode = PythonAppManager.LaunchMode.LOCAL;
    }

    mode = mode != PythonAppManager.LaunchMode.LOCAL ? PythonAppManager.LaunchMode.HADOOP : mode;

    LOG.debug("Launching mode: {} ApexDirectoryPath: {}", mode, this.getApexDirectoryPath());
    this.setRequiredJARFiles();
    this.setRequiredRuntimeFiles();
    this.manager = new PythonAppManager(this, mode);

    DAG dag = this.apexStream.createDag();

    Map<String, String> pythonOperatorEnv = new HashMap<>();
    if (mode == PythonAppManager.LaunchMode.LOCAL) {

      pythonOperatorEnv.put(PythonWorkerContext.PYTHON_WORKER_PATH, this.getApexDirectoryPath() + "/src/pyapex/runtime/" + PythonConstants.PYTHON_WORKER_FILE_NAME);
      pythonOperatorEnv.put(PythonWorkerContext.PY4J_DEPENDENCY_PATH, this.getApexDirectoryPath() + "/deps/" + PythonConstants.PY4J_SRC_ZIP_FILE_NAME);
      pythonOperatorEnv.put(PythonWorkerContext.PYTHON_APEX_PATH, this.getApexDirectoryPath() + "/deps/" + PythonConstants.PY4J_SRC_ZIP_FILE_NAME);

    }

    Collection<DAG.OperatorMeta> operators = dag.getAllOperatorsMeta();
    for (DAG.OperatorMeta operatorMeta : operators) {
      if (operatorMeta.getOperator() instanceof PythonGenericOperator) {
        LOG.debug("Updating python operator: {}" + operatorMeta.getName());
        PythonGenericOperator operator = ((PythonGenericOperator)operatorMeta.getOperator());
        operator.getServer().setPythonOperatorEnv(pythonOperatorEnv);
      }
    }
    return manager.launch();
  }

  public LocalMode.Controller runLocal()
  {
    return this.apexStream.runEmbedded(true, 0, null);
  }

  public ApexStream getApexStream()
  {
    return apexStream;
  }

  public void setApexStream(PythonApexStreamImpl apexStream)
  {
    this.apexStream = apexStream;
  }

  public PythonApp fromFolder(String directoryPath)
  {
    ApexStream currentStream = StreamFactory.fromFolder(directoryPath);
    if (currentStream instanceof ApexStreamImpl) {
      apexStream = new PythonApexStreamImpl<String>((ApexStreamImpl<String>)currentStream);
    }
    return this;
  }

  public PythonApp fromKafka08(String zookeepers, String topic)
  {
    ApexStream currentStream = StreamFactory.fromKafka08(zookeepers, topic);
    if (currentStream instanceof ApexStreamImpl) {
      apexStream = new PythonApexStreamImpl<String>((ApexStreamImpl<String>)currentStream);
    }
    return this;
  }

  public PythonApp fromData(List<Object> inputs)
  {

    ApexStream currentStream = StreamFactory.fromData(inputs);
    if (currentStream instanceof ApexStreamImpl) {
      apexStream = new PythonApexStreamImpl<String>((ApexStreamImpl<String>)currentStream);
    }
    return this;
  }

  public PythonApp fromKafka09(String brokers, String topic)
  {
    ApexStream currentStream = StreamFactory.fromKafka09(brokers, topic, PartitionStrategy.ONE_TO_ONE,1);

    if (currentStream instanceof ApexStreamImpl) {
      apexStream = new PythonApexStreamImpl<String>((ApexStreamImpl<String>)currentStream);
    }
    return this;
  }

  public PythonApp map(String name, byte[] searializedFunction)
  {
    apexStream = (PythonApexStream)apexStream.map(searializedFunction, Option.Options.name(name));

    return this;
  }

  public PythonApp flatMap(String name, byte[] searializedFunction)
  {
    apexStream = (PythonApexStream)apexStream.flatMap(searializedFunction, Option.Options.name(name));
    return this;
  }

  public PythonApp filter(String name, byte[] searializedFunction)
  {
    apexStream = (PythonApexStream)apexStream.filter(searializedFunction, Option.Options.name(name));
    return this;
  }

  public PythonApp window(WindowOption windowOption, TriggerOption triggerOption, Duration allowedLateness)
  {

    apexStream = (PythonApexStream)apexStream.window(windowOption, triggerOption, allowedLateness);
    return this;
  }

  public PythonApp countByKey(String name)
  {

    Function.ToKeyValue<String, String, Long> toKeyValueFunction = new Function.ToKeyValue<String, String, Long>()
    {
      @Override
      public Tuple<KeyValPair<String, Long>> f(String input)
      {
        String[] data = input.split(",");

        return new Tuple.PlainTuple<KeyValPair<String, Long>>(new KeyValPair<String, Long>(data[data.length - 1], 1L));
      }
    };
    if (apexStream instanceof PythonApexStreamImpl) {

      apexStream = (PythonApexStream)((PythonApexStreamImpl)apexStream).countByKey(toKeyValueFunction, Option.Options.name(name));
    }
    return this;
  }

  public PythonApp count(String name)
  {
    if (apexStream instanceof PythonApexStreamImpl) {
      apexStream = (PythonApexStream)((PythonApexStreamImpl)apexStream).count();
    }
    return this;
  }

  public PythonApp reduce(String name, byte[] serializedObject)
  {

    if (apexStream instanceof PythonApexStreamImpl) {
      PythonReduceProxy<String> reduceProxy = new PythonReduceProxy<String>(PythonConstants.OpType.REDUCE, serializedObject, String.class);

      apexStream = (PythonApexStream)((PythonApexStreamImpl)apexStream).reduce(reduceProxy, Option.Options.name(name));
    }
    return this;
  }

  public PythonApp reduceByKey(String name, byte[] serializedObject)
  {
    Function.ToKeyValue<String, String, String> toKeyValueFunction = new Function.ToKeyValue<String, String, String>()
    {
      @Override
      public Tuple<KeyValPair<String, String>> f(String input)
      {
        String[] data = input.split(",");
        return new Tuple.PlainTuple<KeyValPair<String, String>>(new KeyValPair<String, String>(data[0], input));
      }
    };

    if (apexStream instanceof PythonApexStreamImpl) {
      PythonReduceProxy<String> reduceProxy = new PythonReduceProxy<String>(PythonConstants.OpType.REDUCE_BY_KEY, serializedObject, String.class);

      apexStream = (PythonApexStream)((PythonApexStreamImpl)apexStream).reduceByKey(reduceProxy, toKeyValueFunction, Option.Options.name(name));
    }
    return this;
  }

  public PythonApp toConsole(String name)
  {
    apexStream = (PythonApexStream)apexStream.print(Option.Options.name(name));
    return this;
  }

  public PythonApp toKafka08(String name, String topic, Map<String, String> properties)
  {
    KafkaSinglePortOutputOperator kafkaOutputOperator = new KafkaSinglePortOutputOperator();
    kafkaOutputOperator.setTopic(topic);
    List<String> propertyList = new ArrayList<String>();
    for (String key : properties.keySet()) {
      propertyList.add(key + "=" + properties.get(key));
    }

    String producerConfigs = StringUtils.join(propertyList, ",");
    LOG.debug("PropertyList for kafka producer {}" + producerConfigs);
    kafkaOutputOperator.setProducerProperties(producerConfigs);
    apexStream = (PythonApexStream)apexStream.endWith(kafkaOutputOperator, kafkaOutputOperator.inputPort, Option.Options.name(name));
    return this;
  }

  public PythonApp toFolder(String name, String fileName, String directoryName)
  {

    GenericFileOutputOperator<String> outputOperator = new GenericFileOutputOperator<>();
    outputOperator.setFilePath(directoryName);
    outputOperator.setOutputFileName(fileName);
    outputOperator.setConverter(new GenericFileOutputOperator.StringToBytesConverter());
    apexStream = (PythonApexStream)apexStream.endWith(outputOperator, outputOperator.input, Option.Options.name(name));
    return this;
  }

  public PythonApp setConfig(String key, String value)
  {
    getConf().set(key, value);
    return this;
  }

  public void kill() throws Exception
  {
    if (manager == null) {
      throw new Exception("Application is not running yet");

    }
    manager.shutdown();
  }

  public PythonAppManager.LaunchMode getMode()
  {
    return mode;
  }

  public void setMode(PythonAppManager.LaunchMode mode)
  {
    this.mode = mode;
  }
}
