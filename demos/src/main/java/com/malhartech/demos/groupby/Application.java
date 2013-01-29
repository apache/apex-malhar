/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.groupby;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.algo.GroupBy;
import com.malhartech.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

/**
 * In this application, ageInputOperator will generate [id,age] pair [3,23], ...; nameInputOperator will generate [id,name] pair [3,bob], ...
 * SingleJoinOutputOperator will join the age pair and name pair by same id value and output the age name pair. In the above case, it will output [23,bob]
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class Application implements ApplicationFactory
{
  private boolean allInline =  false;

  @Override
  public DAG getApplication(Configuration conf)
  {
    int interval = 100;
    DAG dag = new DAG(conf);

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

    return dag;
  }

}
