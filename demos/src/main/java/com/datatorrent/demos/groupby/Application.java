/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.groupby;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.algo.GroupBy;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import org.apache.hadoop.conf.Configuration;

/**
 * Group Values By Key :<br>
 * This application reads input from two streams marked by ids. Application
 * combines values by id and emits on console port. <br>
 * <br>
 * 
 * Functional Descritpion : <br>
 * Application takes input from ageInputOperator generating map[id, age][3, 23]
 * .......<br>
 * Application takes input from nameInputOperator will generate [id,name] pair
 * [3,bob] ... <br>
 * SingleJoinOutputOperator will join the age pair and name pair by same id
 * value and output the age name pair. <br>
 * In the above case, it will output [23,bob] <br>
 * <br>
 * 
 * Custom Attributes : <br>
 * Sleep interval on input ports is set to 100ms. <br>
 * Key is set to "id" on groupby operator. <br>
 * <br>
 * 
 * Input (s): <br>
 * IdAgeInputOperator -> operator to generate id/age map. <br>
 * IdNameInputOperator -. operator to generate id/name map. <br>
 * 
 * Output(s): <br>
 * Output values are written to console through ConsoleOutputOerator<br>
 * if you need to change write to HDFS,HTTP .. instead of console, <br>
 * Please refer to {@link com.malhartech.lib.io.HttpOutputOperator} or
 * {@link com.malhartech.lib.io.HdfsOutputOperator}. <br>
 * <br>
 * 
 * Run Sample Application : <br>
 * Please consult Application Developer guide <a href=
 * "https://docs.google.com/document/d/1WX-HbsGQPV_DfM1tEkvLm1zD_FLYfdLZ1ivVHqzUKvE/edit#heading=h.lfl6f68sq80m"
 * > here </a>.
 * <p>
 * Running Java Test or Main app in IDE:
 * <pre>
 * LocalMode.runApp(new Application(), 600000); // 10 min run
 * </pre>
 * 
 * Run Success : <br>
 * For successful deployment and run, user should see following output on
 * console:
 * <pre>
 * {id=2, name=allen, age=22}
 * {id=2, name=allen, age=22}
 * {id=2, name=allen, age=22}
 * {id=2, name=allen, age=22}
 * </pre>
 * 
 * Scaling Options : <br>
 * Group by operator is not scalable by setting initial partition size on operator.  <br>
 * State Full : YES, aggregates value on input ports.  <br><br>
 * 
 * Application DAG : <br>
 * <img src="doc-files/Application.gif" width=600px > <br>
 * <br>
 * 
 * Streaming Window Size : 500ms (default) <br>
 * Operator Details : <br>
 * <ul>
 *    <li><p><b> The idAge Operator : </b> Generates map for id/age values after 100ms sleep interval.<br>
 *     Class : {@link com.malhartech.demos.groupby.IdAgeInputOperator} <br>
 *     State Less : Yes, window size 1.
 *    </li> 
 *    <li><p><b> The idName Operator : </b> Generates map for id/name values after 100ms sleep interval.<br>
 *     Class : {@link com.malhartech.demos.groupby.IdNameInputOperator} <br>
 *     State Less : Yes, window size 1.
 *    </li> 
 *    <li><p><b> The groupBy Operator : </b> Generates map for id/age/name  grouped by key 'id'. <br>
 *     Class : {@link com.malhartech.lib.algo.GroupBy} <br>
 *     State Full : Yes, input streams are aggregated acroos window.
 *    </li> 
 *    <li><p><b> The consoleOperator Operator : </b> Prints output to console. </li>
 * </ul>
 * 
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class Application implements StreamingApplication
{
  private final boolean allInline =  false;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    int interval = 100;

    IdAgeInputOperator idAge = dag.addOperator("age", new IdAgeInputOperator());
    IdNameInputOperator idName  = dag.addOperator("name", new IdNameInputOperator());
    idAge.setInterval(interval);
    idName.setInterval(interval);

    GroupBy groupBy = dag.addOperator("groupby", new GroupBy());
    groupBy.setKey("id");

    dag.addStream("age-group", idAge.output, groupBy.data1).setInline(allInline);
    dag.addStream("name-group", idName.output, groupBy.data2 ).setInline(allInline);

    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("group-console",groupBy.groupby, consoleOperator.input).setInline(allInline);

  }

}
