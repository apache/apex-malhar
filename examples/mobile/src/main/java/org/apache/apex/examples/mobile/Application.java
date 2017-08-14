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
package org.apache.apex.examples.mobile;

import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.counters.BasicCounters;
import org.apache.apex.malhar.lib.io.PubSubWebSocketInputOperator;
import org.apache.apex.malhar.lib.io.PubSubWebSocketOutputOperator;
import org.apache.apex.malhar.lib.partitioner.StatelessThroughputBasedPartitioner;
import org.apache.apex.malhar.lib.testbench.RandomEventGenerator;
import org.apache.apex.malhar.lib.utils.PubSubHelper;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.Range;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Mobile Example Application:
 * <p>
 * This example simulates large number of cell phones in the range of 40K to 200K
 * and tracks a given cell number across cell towers. It also displays the changing locations of the cell number on a google map.
 *
 * This example demonstrates the scalability feature of the Apex platform.
 * It showcases the ability of the platform to scale up and down as the phone numbers generated increase and decrease respectively.
 * If the tuples processed per second by the pmove operator increase beyond 30,000, more partitions of the pmove operator gets deployed until
 * each of the partition processes around 10000 to 30000 tuples per second.
 * If the tuples processed per second drops below 10,000, the platform merges the operators until the partition count drops down to the original.
 * The load can be varied using the tuplesBlast property.
 * If the tuplesBlast is set to 200, 40K cell phones are generated.
 * If the tuplesBlast is set to 1000, 200K cell phones are generated.
 * The tuplesBlast property can be set using dtcli command: 'set-operator-property pmove tuplesBlast 1000'.
 *
 *
 * The specs are as such<br>
 * Depending on the tuplesBlast property, large number of cell phone numbers are generated.
 * They jump a cell tower frequently. Sometimes
 * within a second sometimes in 10 seconds. The aim is to demonstrate the
 * following abilities<br>
 * <ul>
 * <li>Entering query dynamically: The phone numbers are added to locate its gps
 * in run time.</li>
 * <li>Changing functionality dynamically: The load is changed by making
 * functional changes on the load generator operator (phonegen)(</li>
 * <li>Auto Scale up/Down with load: Operator pmove increases and decreases
 * partitions as per load</li>
 * <li></li>
 * </ul>
 *
 * Refer to examples/docs/MobileDemo.md for more information.
 *
 * <p>
 *
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
 * phoneLocationQueryResult: {phone=5556101, location=(5,9), queryId=q3}
 * phoneLocationQueryResult: {phone=5554995, location=(10,4), queryId=q1}
 * phoneLocationQueryResult: {phone=5556101, location=(5,9), queryId=q3}
 * phoneLocationQueryResult: {phone=5554995, location=(10,4), queryId=q1}
 * phoneLocationQueryResult: {phone=5554995, location=(10,5), queryId=q1}
 * phoneLocationQueryResult: {phone=5556101, location=(5,9), queryId=q3}
 * phoneLocationQueryResult: {phone=5554995, location=(9,5), queryId=q1}
 * phoneLocationQueryResult: {phone=5556101, location=(5,9), queryId=q3}
 * phoneLocationQueryResult: {phone=5556101, location=(5,9), queryId=q3}
 * phoneLocationQueryResult: {phone=5554995, location=(9,5), queryId=q1}
 * phoneLocationQueryResult: {phone=5554995, location=(9,5), queryId=q1}
 * phoneLocationQueryResult: {phone=5556101, location=(5,9), queryId=q3}
 * </pre>
 *
 * * <b>Application DAG : </b><br>
 * <img src="doc-files/mobile.png" width=600px > <br>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name = "MobileExample")
public class Application implements StreamingApplication
{
  public static final String PHONE_RANGE_PROP = "dt.application.MobileExample.phoneRange";
  public static final String TOTAL_SEED_NOS = "dt.application.MobileExample.totalSeedNumbers";
  public static final String COOL_DOWN_MILLIS = "dt.application.MobileExample.coolDownMillis";
  public static final String MAX_THROUGHPUT = "dt.application.MobileExample.maxThroughput";
  public static final String MIN_THROUGHPUT = "dt.application.MobileExample.minThroughput";
  private static final Logger LOG = LoggerFactory.getLogger(Application.class);
  private Range<Integer> phoneRange = Range.between(5550000, 5559999);

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String lPhoneRange = conf.get(PHONE_RANGE_PROP, null);
    if (lPhoneRange != null) {
      String[] tokens = lPhoneRange.split("-");
      if (tokens.length != 2) {
        throw new IllegalArgumentException("Invalid range: " + lPhoneRange);
      }
      this.phoneRange = Range.between(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
    }
    LOG.debug("Phone range {}", this.phoneRange);

    RandomEventGenerator phones = dag.addOperator("Receiver", RandomEventGenerator.class);
    phones.setMinvalue(this.phoneRange.getMinimum());
    phones.setMaxvalue(this.phoneRange.getMaximum());

    PhoneMovementGenerator movementGen = dag.addOperator("LocationFinder", PhoneMovementGenerator.class);
    dag.setAttribute(movementGen, OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    StatelessThroughputBasedPartitioner<PhoneMovementGenerator> partitioner = new StatelessThroughputBasedPartitioner<PhoneMovementGenerator>();
    partitioner.setCooldownMillis(conf.getLong(COOL_DOWN_MILLIS, 45000));
    partitioner.setMaximumEvents(conf.getLong(MAX_THROUGHPUT, 30000));
    partitioner.setMinimumEvents(conf.getLong(MIN_THROUGHPUT, 10000));
    dag.setAttribute(movementGen, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{partitioner}));
    dag.setAttribute(movementGen, OperatorContext.PARTITIONER, partitioner);

    // generate seed numbers
    Random random = new Random();
    int maxPhone = phoneRange.getMaximum() - phoneRange.getMinimum();
    int phonesToDisplay = conf.getInt(TOTAL_SEED_NOS, 10);
    for (int i = phonesToDisplay; i-- > 0; ) {
      int phoneNo = phoneRange.getMinimum() + random.nextInt(maxPhone + 1);
      LOG.info("seed no: " + phoneNo);
      movementGen.phoneRegister.add(phoneNo);
    }
    // done generating data
    LOG.info("Finished generating seed data.");

    URI uri = PubSubHelper.getURI(dag);
    PubSubWebSocketOutputOperator<Object> wsOut = dag.addOperator("LocationResults", new PubSubWebSocketOutputOperator<Object>());
    wsOut.setUri(uri);
    PubSubWebSocketInputOperator<Map<String, String>> wsIn = dag.addOperator("QueryLocation", new PubSubWebSocketInputOperator<Map<String, String>>());
    wsIn.setUri(uri);
    // default partitioning: first connected stream to movementGen will be partitioned
    dag.addStream("Phone-Data", phones.integer_data, movementGen.data);
    dag.addStream("Results", movementGen.locationQueryResult, wsOut.input);
    dag.addStream("Query", wsIn.outputPort, movementGen.phoneQuery);
  }

}
