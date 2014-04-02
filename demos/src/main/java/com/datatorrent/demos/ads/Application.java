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
package com.datatorrent.demos.ads;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;
import com.datatorrent.lib.math.MarginMap;
import com.datatorrent.lib.math.QuotientMap;
import com.datatorrent.lib.math.SumCountMap;
import com.datatorrent.lib.stream.StreamMerger;
import com.datatorrent.lib.testbench.EventClassifier;
import com.datatorrent.lib.testbench.EventGenerator;
import com.datatorrent.lib.testbench.FilteredEventClassifier;
import com.datatorrent.lib.testbench.ThroughputCounter;

/**
 * <p>
 * This demo shows live computation of cost/revenue/margin/ctr for ads for
 * various advertisers. <br>
 * <b>Functional Description : </b><br>
 * Application generate ads events data stamped by advertiser name and clicks. <br>
 * Core of application aggregates revenue/cost for clicks converted into sale
 * and total clicks for ads by advertiser name. <br>
 * Application outputs cost/revenue/margin/ctr by for every advertiser.<br>
 * <br>
 * <b>Input(s) : </b><br>
 * Random event generator and tuple stamper. <br>
 * <br>
 * <b>Output(s) : </b><br>
 * Output Adapter : <br>
 * Output values are written to console through ConsoleOutputOerator<br>
 * If needed you can use other output adapters<br>
 * <br>
 * <p>
 * Running Java Test or Main app in IDE:
 *
 * <pre>
 * LocalMode.runApp(new Application(), 600000); // 10 min run
 * </pre>
 *
 * Run Success : <br>
 * For successful deployment and run, user should see following output on
 * console: <br>
 *
 * <pre>
 * costConsole{}
 * revConsole{}
 * costConsole{}
 * revConsole{}
 * costConsole{}
 * revConsole{}
 * costConsole{sprint,finance=1050.2310000048753, nike,sports=599.9577500021693, sprint,mail=201.57719999946136, sprint,sports=611.1157500022679, etrade,mail=199.77719999948687, nike,mail=197.5589999995183, nike,home=766.9652000000992, sprint,home=780.1425500002754, etrade,home=773.4969000001865, etrade,sports=605.2130000022157, nike,finance=1028.5290000053885, etrade,finance=1038.3360000051566}
 * ctrConsole{sprint,mail=100.0, nike,sports=100.0, sprint,finance=100.0, etrade,mail=100.0, sprint,sports=100.0, nike,home=100.0, nike,mail=100.0, sprint,home=100.0, etrade,sports=100.0, etrade,home=100.0, nike,finance=100.0, etrade,finance=100.0}
 * revConsole{nike,sports=134052.0, sprint,mail=19636.0, sprint,finance=4063.0, etrade,mail=29880.0, sprint,sports=4102.0, nike,mail=55892.0, nike,home=52396.0, sprint,home=25696.0, etrade,sports=20040.0, etrade,home=21170.0, nike,finance=24552.0, etrade,finance=156545.0}
 * viewCountConsole{count=4320197, window_time=1982, tuples_per_sec=2179715, window_id=2, avg=1748380}
 * marginConsole{sprint,finance=74.15134137324944, sprint,mail=98.97343043389967, nike,sports=99.5524440142615, sprint,sports=85.10200511939864, etrade,mail=99.33140160642742, nike,home=98.53621421482536, nike,mail=99.64653438774866, sprint,home=96.96395333904002, etrade,home=96.3462593292386, etrade,sports=96.97997504988915, nike,finance=95.81081378296925, etrade,finance=99.33671723785163}
 * costConsole{}
 * revConsole{}
 * costConsole{}
 * </pre>
 * <br>
 * Scaling Options : <br>
 * User set partitions on sum operator, refer code {@link com.datatorrent.lib.math.SumCountMap} <br>
 * <br>
 * <b>Application DAG : </b><br>
 * <img src="doc-files/AdsDemo.png" width=600px > <br>
 * <b> Streaming Window Size : </b> 1000(500 seconds). <br>
 * <br>
 * <b>Operator Details: </b> <br>
 *  <ul>
 * 	<li>
 *     <b>  viewGen : </b> This is random event generator for  ads view data.
 *         This can replaced by any input stream. by user. <br>
 *     Class : {@link com.datatorrent.lib.testbench.EventGenerator}  <br>
 *     Operator Application Window Count : 1 <br>
 *     StateFull : No
 *  </li>
 *  <li>
 *  <b> adviews : </b> This is operator stamps random data with advertiser name,
 *         This can replaced by any input stream. by user. <br>
 *     Class : {@link com.datatorrent.lib.testbench.EventClassifier}  <br>
 *     Operator Application Window Count : 1 <br>
 *     StateFull : No
 *  </li>
 *  <li>
 *  <b> insertClicks : </b> This operator convert stamped stream into clicks data. <br>
 *     Class : {@link com.datatorrent.lib.testbench.FilteredEventClassifier} <br>
 *     Operator Application Window Count : 1 <br>
 *     StateFull : No <br>
 *  </li>
 *  <li>
 *  <b> viewAggregate : </b> Operator sums revenue/cost data for ads converted to sale. <br>
 *      Class : {@link com.datatorrent.lib.math.SumCountMap} <br>
 *      Operator Application Window Count : 1000<br>
 *      StateFull : Yes <br>
 *  </li>
 *  <li>
 *  <b> clicksAggregate : </b> Operator sums clicks for ads by advertiser. <br>
 *      Class : {@link com.datatorrent.lib.math.SumCountMap} <br>
 *      Operator Application Window Count : 1000<br>
 *      StateFull : Yes <br>
 *  </li>
 *  <li>
 *  <b> margin : </b> Operator computes cost/revenu margin. <br>
 *      Class : {@link import com.datatorrent.lib.math.MarginMap} <br>
 *      Operator Application Window Count : 1 <br>
 *      StateFull : No <br>
 *  </li>
 *  <li>
 *  <b> ctr : </b> Computes quotient for sales/total clicks. <br>
 *     Class : {@link import com.datatorrent.lib.math.QuotientMap} <br>
 *      Operator Application Window Count : 1 <br>
 *      StateFull : No <br>
 *  </li>
 *  </ul>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name="AdsApplication")
public class Application implements StreamingApplication
{
  public static final int WINDOW_SIZE_MILLIS = 500;
  public static final String P_numGenerators = Application.class.getName() + ".numGenerators";
  public static final String P_generatorVTuplesBlast = Application.class.getName() + ".generatorVTuplesBlast";
  public static final String P_generatorMaxWindowsCount = Application.class.getName() + ".generatorMaxWindowsCount";
  public static final String P_allInline = Application.class.getName() + ".allInline";
  public static final String P_enableHdfs = Application.class.getName() + ".enableHdfs";
  // adjust these depending on execution mode (junit, cli-local, cluster)
  private int applicationWindow = 5 * 1000 / WINDOW_SIZE_MILLIS;
  private int generatorVTuplesBlast = 1000;
  private int generatorMaxWindowsCount = 100;
  private int generatorWindowCount = 1;
  private Locality locality = null;
  private int numGenerators = 1;

  public void setUnitTestMode()
  {
    generatorVTuplesBlast = 10;
    generatorWindowCount = 5;
    generatorMaxWindowsCount = 20;
    applicationWindow = 5;
  }

  public void setLocalMode()
  {
    generatorVTuplesBlast = 1000; // keep low to not distort window boundaries
    //generatorVTuplesBlast = 500000;
    generatorWindowCount = 5;
    //generatorMaxWindowsCount = 50;
    generatorMaxWindowsCount = 1000000;
  }

  private void configure(DAG dag, Configuration conf)
  {

    if (LAUNCHMODE_YARN.equals(conf.get(DAG.LAUNCH_MODE))) {
      setLocalMode();
      // settings only affect distributed mode
      AttributeMap attributes = dag.getAttributes();
      if (attributes.get(DAGContext.CONTAINER_MEMORY_MB) == null) {
        attributes.put(DAGContext.CONTAINER_MEMORY_MB, 2048);
      }
      if (attributes.get(DAGContext.MASTER_MEMORY_MB) == null) {
        attributes.put(DAGContext.MASTER_MEMORY_MB, 1024);
      }
      if (attributes.get(DAGContext.CONTAINERS_MAX_COUNT) == null) {
        attributes.put(DAGContext.CONTAINERS_MAX_COUNT, 1);
      }
    }
    else if (LAUNCHMODE_LOCAL.equals(conf.get(DAG.LAUNCH_MODE))) {
      setLocalMode();
    }

    this.generatorVTuplesBlast = conf.getInt(P_generatorVTuplesBlast, this.generatorVTuplesBlast);
    this.generatorMaxWindowsCount = conf.getInt(P_generatorMaxWindowsCount, this.generatorMaxWindowsCount);
    this.locality = conf.getBoolean(P_allInline, false) ? Locality.CONTAINER_LOCAL : null;
    this.numGenerators = conf.getInt(P_numGenerators, this.numGenerators);

  }

  private InputPort<Object> getConsolePort(DAG b, String name, boolean silent)
  {
    // output to HTTP server when specified in environment setting
    String gatewayAddress = b.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    if (!StringUtils.isEmpty(gatewayAddress)) {
      URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
      String topic = "demos.ads." + name;
      //LOG.info("WebSocket with gateway at: {}", gatewayAddress);
      PubSubWebSocketOutputOperator<Object> wsOut = b.addOperator(name, new PubSubWebSocketOutputOperator<Object>());
      wsOut.setUri(uri);
      wsOut.setTopic(topic);
      return wsOut.input;
    }
    ConsoleOutputOperator oper = b.addOperator(name, new ConsoleOutputOperator());
    oper.setStringFormat(name + "%s");
    oper.silent = silent;
    return oper.input;
  }

  public SumCountMap<String, Double> getSumOperator(String name, DAG b)
  {
    return b.addOperator(name, new SumCountMap<String, Double>());
  }

  public StreamMerger<HashMap<String, Integer>> getStreamMerger(String name, DAG b)
  {
    StreamMerger<HashMap<String, Integer>> oper = b.addOperator(name, new StreamMerger<HashMap<String, Integer>>());
    return oper;
  }

  public ThroughputCounter<String, Integer> getThroughputCounter(String name, DAG b)
  {
    ThroughputCounter<String, Integer> oper = b.addOperator(name, new ThroughputCounter<String, Integer>());
    oper.setRollingWindowCount(5);
    return oper;
  }

  public MarginMap<String, Double> getMarginOperator(String name, DAG b)
  {
    MarginMap<String, Double> oper = b.addOperator(name, new MarginMap<String, Double>());
    oper.setPercent(true);
    return oper;
  }

  public QuotientMap<String, Integer> getQuotientOperator(String name, DAG b)
  {
    QuotientMap<String, Integer> oper = b.addOperator(name, new QuotientMap<String, Integer>());
    oper.setMult_by(100);
    oper.setCountkey(true);
    return oper;
  }

  public EventGenerator getPageViewGenOperator(String name, DAG b)
  {
    EventGenerator oper = b.addOperator(name, EventGenerator.class);
    oper.setKeys("home,finance,sports,mail");
    // Paying $2.15,$3,$1.75,$.6 for 1000 views respectively
    oper.setValues("0.00215,0.003,0.00175,0.0006");
    oper.setWeights("25,25,25,25");
    oper.setTuplesBlast(this.generatorVTuplesBlast);
    oper.setMaxCountOfWindows(generatorMaxWindowsCount);
    oper.setRollingWindowCount(this.generatorWindowCount);
    return oper;
  }

  public EventClassifier getAdViewsStampOperator(String name, DAG b)
  {
    EventClassifier oper = b.addOperator(name, EventClassifier.class);
    HashMap<String, Double> kmap = new HashMap<String, Double>();
    kmap.put("sprint", null);
    kmap.put("etrade", null);
    kmap.put("nike", null);
    oper.setKeyMap(kmap);
    return oper;
  }

  public FilteredEventClassifier<Double> getInsertClicksOperator(String name, DAG b)
  {
    FilteredEventClassifier<Double> oper = b.addOperator(name, new FilteredEventClassifier<Double>());
    HashMap<String, Double> kmap = new HashMap<String, Double>();
    // Getting $1,$5,$4 per click respectively
    kmap.put("sprint", 1.0);
    kmap.put("etrade", 5.0);
    kmap.put("nike", 4.0);
    oper.setKeyMap(kmap);

    HashMap<String, ArrayList<Integer>> wmap = new HashMap<String, ArrayList<Integer>>();
    ArrayList<Integer> alist = new ArrayList<Integer>(3);
    alist.add(60);
    alist.add(10);
    alist.add(30);
    wmap.put("home", alist);
    alist = new ArrayList<Integer>(3);
    alist.add(10);
    alist.add(75);
    alist.add(15);
    wmap.put("finance", alist);
    alist = new ArrayList<Integer>(3);
    alist.add(10);
    alist.add(10);
    alist.add(80);
    wmap.put("sports", alist);
    alist = new ArrayList<Integer>(3);
    alist.add(50);
    alist.add(15);
    alist.add(35);
    wmap.put("mail", alist);
    oper.setKeyWeights(wmap);
    oper.setPassFilter(40);
    oper.setTotalFilter(1000);
    return oper;
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    configure(dag, conf);
    dag.setAttribute(DAG.APPLICATION_NAME, "AdsApplication");
    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, WINDOW_SIZE_MILLIS); // set the streaming window size to 1 millisec

    //dag.getAttributes().attr(DAG.CONTAINERS_MAX_COUNT).setIfAbsent(9);
    EventGenerator viewGen = getPageViewGenOperator("viewGen", dag);
    dag.getMeta(viewGen).getAttributes().put(OperatorContext.INITIAL_PARTITION_COUNT, numGenerators);
    dag.setOutputPortAttribute(viewGen.hash_data, PortContext.QUEUE_CAPACITY, 32 * 1024);

    EventClassifier adviews = getAdViewsStampOperator("adviews", dag);
    dag.setOutputPortAttribute(adviews.data, PortContext.QUEUE_CAPACITY, 32 * 1024);
    dag.setInputPortAttribute(adviews.event, PortContext.QUEUE_CAPACITY, 32 * 1024);

    FilteredEventClassifier<Double> insertclicks = getInsertClicksOperator("insertclicks", dag);
    dag.setInputPortAttribute(insertclicks.data, PortContext.QUEUE_CAPACITY, 32 * 1024);

    SumCountMap<String, Double> viewAggregate = getSumOperator("viewAggr", dag);
    dag.setAttribute(viewAggregate, OperatorContext.APPLICATION_WINDOW_COUNT, applicationWindow);
    dag.setInputPortAttribute(viewAggregate.data, PortContext.QUEUE_CAPACITY, 32 * 1024);

    SumCountMap<String, Double> clickAggregate = getSumOperator("clickAggr", dag);
    dag.setAttribute(clickAggregate, OperatorContext.APPLICATION_WINDOW_COUNT, applicationWindow);

    dag.setInputPortAttribute(adviews.event, PortContext.PARTITION_PARALLEL, true);
    dag.addStream("views", viewGen.hash_data, adviews.event).setLocality(Locality.CONTAINER_LOCAL);
    dag.setInputPortAttribute(insertclicks.data, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(viewAggregate.data, PortContext.PARTITION_PARALLEL, true);
    DAG.StreamMeta viewsAggStream = dag.addStream("viewsaggregate", adviews.data, insertclicks.data, viewAggregate.data).setLocality(Locality.CONTAINER_LOCAL);

    if (conf.getBoolean(P_enableHdfs, false)) {
      HdfsHashMapOutputOperator viewsToHdfs = dag.addOperator("viewsToHdfs", new HdfsHashMapOutputOperator());
      viewsToHdfs.setAppend(false);
      viewsToHdfs.setCloseCurrentFile(true);
      viewsToHdfs.setFilePathPattern("file:///tmp/adsdemo/views-%(operatorId)-part%(partIndex)");
      dag.setInputPortAttribute(viewsToHdfs.input, PortContext.PARTITION_PARALLEL, true);
      viewsAggStream.addSink(viewsToHdfs.input);
    }

    dag.setInputPortAttribute(clickAggregate.data, PortContext.PARTITION_PARALLEL, true);
    dag.addStream("clicksaggregate", insertclicks.filter, clickAggregate.data).setLocality(Locality.CONTAINER_LOCAL);


    QuotientMap<String, Integer> ctr = getQuotientOperator("ctr", dag);
    SumCountMap<String, Double> cost = getSumOperator("cost", dag);
    SumCountMap<String, Double> revenue = getSumOperator("rev", dag);
    MarginMap<String, Double> margin = getMarginOperator("margin", dag);
    StreamMerger<HashMap<String, Integer>> merge = getStreamMerger("countmerge", dag);
    ThroughputCounter<String, Integer> tuple_counter = getThroughputCounter("tuple_counter", dag);

    dag.addStream("adviewsdata", viewAggregate.sum, cost.data);
    dag.addStream("clicksdata", clickAggregate.sum, revenue.data);
    dag.addStream("viewtuplecount", viewAggregate.count, ctr.denominator, merge.data1).setLocality(locality);
    dag.addStream("clicktuplecount", clickAggregate.count, ctr.numerator, merge.data2).setLocality(locality);
    dag.addStream("total count", merge.out, tuple_counter.data).setLocality(locality);

    InputPort<Object> revconsole = getConsolePort(dag, "revConsole", false);
    InputPort<Object> costconsole = getConsolePort(dag, "costConsole", false);
    InputPort<Object> marginconsole = getConsolePort(dag, "marginConsole", false);
    InputPort<Object> ctrconsole = getConsolePort(dag, "ctrConsole", false);
    InputPort<Object> viewcountconsole = getConsolePort(dag, "viewCountConsole", false);

    dag.addStream("revenuedata", revenue.sum, margin.denominator, revconsole).setLocality(locality);
    dag.addStream("costdata", cost.sum, margin.numerator, costconsole).setLocality(locality);
    dag.addStream("margindata", margin.margin, marginconsole).setLocality(locality);
    dag.addStream("ctrdata", ctr.quotient, ctrconsole).setLocality(locality);
    dag.addStream("tuplecount", tuple_counter.count, viewcountconsole).setLocality(locality);

  }

}
