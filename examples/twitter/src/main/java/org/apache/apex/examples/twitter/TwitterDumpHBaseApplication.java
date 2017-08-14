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
package org.apache.apex.examples.twitter;

import java.nio.ByteBuffer;

import org.apache.apex.malhar.contrib.hbase.AbstractHBasePutOutputOperator;
import org.apache.apex.malhar.contrib.twitter.TwitterSampleInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import twitter4j.Status;

/**
 * An application which connects to Twitter Sample Input and stores all the
 * tweets with their usernames in a hbase database. Please review the docs
 * for TwitterTopCounterApplication to setup your twitter credentials.
 *
 * You need to create the HBase table to run this example. Table name can be
 * configured but columnfamily must be 'cf' to make this example simple and complied
 * with the mysql based example.
 * create 'tablename', 'cf'
 *
 * </pre>
 *
 * @since 1.0.2
 */
@ApplicationAnnotation(name = "TwitterDumpHBaseExample")
public class TwitterDumpHBaseApplication implements StreamingApplication
{

  public static class Status2Hbase extends AbstractHBasePutOutputOperator<Status>
  {

    @Override
    public Put operationPut(Status t)
    {
      Put put = new Put(ByteBuffer.allocate(8).putLong(t.getCreatedAt().getTime()).array());
      put.add("cf".getBytes(), "text".getBytes(), t.getText().getBytes());
      put.add("cf".getBytes(), "userid".getBytes(), t.getText().getBytes());
      return put;
    }

  }


  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //dag.setAttribute(DAGContext.APPLICATION_NAME, "TweetsDump");

    TwitterSampleInput twitterStream = dag.addOperator("TweetSampler", new TwitterSampleInput());

    Status2Hbase hBaseWriter = dag.addOperator("DatabaseWriter", new Status2Hbase());

    dag.addStream("Statuses", twitterStream.status, hBaseWriter.input).setLocality(Locality.CONTAINER_LOCAL);
  }

}
