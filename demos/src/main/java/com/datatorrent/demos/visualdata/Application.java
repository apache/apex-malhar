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
package com.datatorrent.demos.visualdata;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.demos.pi.PiCalculateOperator;
import com.datatorrent.lib.io.WidgetOutputOperator;
import com.datatorrent.lib.testbench.RandomEventGenerator;

/**
 * Visual data demo.
 *
 * @since 0.9.3
 */
@ApplicationAnnotation(name="VisualDataApplication")
public class Application implements StreamingApplication {
    
  private final Locality locality = Locality.CONTAINER_LOCAL;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String appName = conf.get("appName");
    if(appName == null){
      appName = "VisualDataDemo";
    }
    dag.setAttribute(DAG.APPLICATION_NAME, appName);
    int maxValue = 30000;

    RandomEventGenerator rand = dag.addOperator("random", new RandomEventGenerator());
    rand.setMinvalue(0);
    rand.setMaxvalue(maxValue);

    DemoValueGenerator demo = dag.addOperator("chartValue", new DemoValueGenerator());
    demo.setRandomIncrement(5);
    demo.setRandomIncrement2(20);

    PiCalculateOperator calc = dag.addOperator("picalc", new PiCalculateOperator());
    calc.setBase(maxValue * maxValue);
    dag.addStream("rand_calc", rand.integer_data, calc.input).setLocality(locality);

    WidgetOutputOperator woo = dag.addOperator("widget output operator", new WidgetOutputOperator());
    WidgetOutputOperator wooa = dag.addOperator("widget output operator2", new WidgetOutputOperator());

    // wire to simple input gadget
    dag.addStream("ws_pi_data", calc.output, woo.simpleInput.setTopic("app." + appName + ".piValue")).setLocality(locality);

    // wire to time series chart gadget
    dag.addStream("ws_chart_data", demo.simpleOutput, woo.timeSeriesInput.setTopic("app." + appName + ".chartValue").setMin(0).setMax(100)).setLocality(locality);

    // wire to another time series chart gadget
    dag.addStream("ws_chart_data2", demo.simpleOutput2, wooa.timeSeriesInput.setTopic("app." + appName + ".chartValue2")).setLocality(locality);

    // wire to percentage chart gadget
    dag.addStream("ws_percentage_data", demo.percentageOutput, woo.percentageInput.setTopic("app." + appName + ".percentage")).setLocality(locality);

    // wire to top N chart gadget
    dag.addStream("ws_topn_data", demo.top10Output, woo.topNInput.setN(10).setTopic("app." + appName + ".topn")).setLocality(locality);

    // wire to progress bar chart gadget
    dag.addStream("ws_progress_data", demo.progressOutput, wooa.percentageInput.setTopic("app." + appName + ".progress")).setLocality(locality);
    
    // wire to piechart gadget
    dag.addStream("ws_piechart_data", demo.pieChartOutput, wooa.pieChartInput.setTopic("app." + appName + ".piechart")).setLocality(locality);
    
  }

}
