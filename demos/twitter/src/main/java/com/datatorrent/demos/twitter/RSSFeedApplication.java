/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.twitter;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name = "RSSFeedApplication")
public class RSSFeedApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RSSFeedExtractor RSSExtractor = dag.addOperator("RSSExtractor",
        RSSFeedExtractor.class);

    ConsoleOutputOperator consoleOperator = dag.addOperator("topWords",
        new ConsoleOutputOperator());
    dag.addStream("TopWords", RSSExtractor.output, consoleOperator.input);
  }
}
