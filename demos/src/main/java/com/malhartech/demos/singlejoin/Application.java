/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.singlejoin;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

/**
 * In this application, ageGenerator will generate [id,age] pair [3,23], ...; nameGenerator will generate [id,name] pair [3,bob], ...
 * SingleJoinBolt will join the age pair and name pair by same id value and output the age name pair. In the above case, it will output [23,bob]
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class Application implements ApplicationFactory
{
  private boolean allInline =  false;

  @Override
  public DAG getApplication(Configuration conf)
  {
    DAG dag = new DAG(conf);

    AgeGenerator ageGen = dag.addOperator("age", new AgeGenerator());
    NameGenerator nameGen  = dag.addOperator("name", new NameGenerator());

    SingleJoinBolt joinBolt = dag.addOperator("join", new SingleJoinBolt());

    dag.addStream("age-join", ageGen.output, joinBolt.age).setInline(allInline);
    dag.addStream("name-join", nameGen.output, joinBolt.name ).setInline(allInline);

    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("count-console",joinBolt.output, consoleOperator.input).setInline(allInline);

    return dag;
  }

}
