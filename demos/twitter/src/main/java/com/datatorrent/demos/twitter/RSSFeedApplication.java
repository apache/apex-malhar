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

/**
 * This application parses most online RSS Feeds.
 * If you want to change the feed being parsed, it can be changed by editing the RSS.xml file in the resources folder.
 * {@link com.datatorrent.demos.twitter.RSSFeedApplication} <br>
 * Run Sample :
 *
 * <pre>
 * TITLE : Bright, Young, In Limbo: Film Sees Migrant Farm Life Through A Child's Eyes
 * DESCRIPTION : José Anzaldo is a third-grader who is a math whiz. He's also the son of itinerant lettuce pickers. A new documentary explores what might become of this promising boy.
 * PUBDATE : Mon, 28 Dec 2015 14:57:00 -0500
 * LINK : http://www.npr.org/sections/thesalt/2015/12/28/459821142/bright-young-undocumented-migrant-farm-life-through-a-childs-eyes?utm_medium=RSS&utm_campaign=business
 * GUID : http://www.npr.org/sections/thesalt/2015/12/28/459821142/bright-young-undocumented-migrant-farm-life-through-a-childs-eyes?utm_medium=RSS&utm_campaign=business
 * CONTENT:ENCODED : <p>José Anzaldo is a third-grader who is a math whiz. He's also the son of itinerant lettuce pickers. A new documentary explores what might become of this promising boy.</p>
 * DC:CREATOR : Maria Godoy
 * 
 * -----------------------------------
 * 
 * TITLE : Investor Hindsight: Lego Sets Are Better Than Gold
 * DESCRIPTION : Looking over investment opportunities since the millennium, <em>The Telegraph</em> newspaper reports that gold brought in an annual average return of 9.5 percent. Lego sets returned an average of 12 percent.
 * PUBDATE : Mon, 28 Dec 2015 07:02:28 -0500
 * LINK : http://www.npr.org/2015/12/28/461247396/investor-hindsight-lego-sets-are-better-than-gold?utm_medium=RSS&utm_campaign=business
 * GUID : http://www.npr.org/2015/12/28/461247396/investor-hindsight-lego-sets-are-better-than-gold?utm_medium=RSS&utm_campaign=business
 * CONTENT:ENCODED : <p>Looking over investment opportunities since the millennium, <em>The Telegraph</em> newspaper reports that gold brought in an annual average return of 9.5 percent. Lego sets returned an average of 12 percent.</p>
 * 
 * -----------------------------------
 * </pre>
 *
 * @since 2.0.0
 */
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
