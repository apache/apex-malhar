/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.groupby;

import com.datatorrent.lib.algo.GroupBy;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;

import org.apache.hadoop.conf.Configuration;

/**
 * In this application, ageInputOperator will generate [id,age] pair [3,23], ...; nameInputOperator will generate [id,name] pair [3,bob], ...
 * SingleJoinOutputOperator will join the age pair and name pair by same id value and output the age name pair. In the above case, it will output [23,bob]
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class Application implements ApplicationFactory
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
