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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;

import twitter4j.Status;

import com.datatorrent.contrib.hbase.HBaseOutputOperator;
import com.datatorrent.contrib.hbase.HBaseRowStatePersistence;
import com.datatorrent.contrib.hbase.HBaseStatePersistenceStrategy;
import com.datatorrent.contrib.twitter.TwitterSampleInput;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * An application which connects to Twitter Sample Input and stores all the
 * tweets with their usernames in a hbase database. Please review the docs
 * for TwitterTopCounterApplication to setup your twitter credentials.
 *
 * You need to create the HBase table to run this demo. Table name can be
 * configured but columnfamily must be 'cf' to make this demo simple and complied
 * with the mysql based demo.
 * create 'tablename', 'cf'
 *
 * </pre>
 *
 * @since 1.0.2
 */
@ApplicationAnnotation(name="TwitterDumpHBaseDemo")
public class TwitterDumpHBaseApplication implements StreamingApplication
{

  public static class Status2Hbase extends HBaseOutputOperator<Status>{

    @Override
    public HBaseStatePersistenceStrategy getPersistenceStrategy()
    {
      return new HBaseRowStatePersistence();
    }

    @Override
    public void processTuple(Status t) throws IOException
    {
      Put put = new Put(ByteBuffer.allocate(8).putLong(t.getCreatedAt().getTime()).array());
      put.add("cf".getBytes(), "text".getBytes(), t.getText().getBytes());
      put.add("cf".getBytes(), "userid".getBytes(), t.getText().getBytes());
      getTable().put(put);
    }

  }


  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //dag.setAttribute(DAGContext.APPLICATION_NAME, "TweetsDump");

    TwitterSampleInput twitterStream = dag.addOperator("TweetSampler", new TwitterSampleInput());

    Status2Hbase hBaseWriter = dag.addOperator("DatabaseWriter", new Status2Hbase());

    dag.addStream("Statuses", twitterStream.status, hBaseWriter.inputPort).setLocality(Locality.CONTAINER_LOCAL);
  }

}
