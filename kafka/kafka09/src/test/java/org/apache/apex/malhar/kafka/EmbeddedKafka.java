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
package org.apache.apex.malhar.kafka;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.base.Throwables;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

public class EmbeddedKafka
{
  private static final String KAFKA_PATH = "/tmp/kafka-test";

  private ZkClient zkClient;
  private ZkUtils zkUtils;
  private String BROKERHOST = "127.0.0.1";
  private String BROKERPORT = "9092";
  private EmbeddedZookeeper zkServer;
  private KafkaServer kafkaServer;

  public String getBroker()
  {
    return BROKERHOST + ":" + BROKERPORT;
  }

  public void start() throws IOException
  {
    // Find port
    try {
      ServerSocket serverSocket = new ServerSocket(0);
      BROKERPORT = Integer.toString(serverSocket.getLocalPort());
      serverSocket.close();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    // Setup Zookeeper
    zkServer = new EmbeddedZookeeper();
    String zkConnect = BROKERHOST + ":" + zkServer.port();
    zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
    zkUtils = ZkUtils.apply(zkClient, false);

    // Setup brokers
    cleanupDir();
    Properties props = new Properties();
    props.setProperty("zookeeper.connect", zkConnect);
    props.setProperty("broker.id", "0");
    props.setProperty("log.dirs", KAFKA_PATH);
    props.setProperty("listeners", "PLAINTEXT://" + BROKERHOST + ":" + BROKERPORT);
    KafkaConfig config = new KafkaConfig(props);
    Time mock = new MockTime();
    kafkaServer = TestUtils.createServer(config, mock);
  }

  public void stop() throws IOException
  {
    kafkaServer.shutdown();
    zkClient.close();
    zkServer.shutdown();
    cleanupDir();
  }

  private void cleanupDir() throws IOException
  {
    FileUtils.deleteDirectory(new File(KAFKA_PATH));
  }

  public void createTopic(String topic)
  {
    AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties());
    List<KafkaServer> servers = new ArrayList<KafkaServer>();
    servers.add(kafkaServer);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 30000);
  }

  public void publish(String topic, List<String> messages)
  {
    Properties producerProps = new Properties();
    producerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
    producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    try (KafkaProducer<Integer, byte[]> producer = new KafkaProducer<>(producerProps)) {
      for (String message : messages) {
        ProducerRecord<Integer, byte[]> data = new ProducerRecord<>(topic, message.getBytes(StandardCharsets.UTF_8));
        producer.send(data);
      }
    }

    List<KafkaServer> servers = new ArrayList<KafkaServer>();
    servers.add(kafkaServer);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 30000);
  }

  public List<String> consume(String topic, int timeout)
  {
    return consume(topic, timeout, true);
  }

  public List<String> consume(String topic, int timeout, boolean earliest)
  {
    Properties consumerProps = new Properties();
    consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
    consumerProps.setProperty("group.id", "group0");
    consumerProps.setProperty("client.id", "consumer0");
    consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
    consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    // to make sure the consumer starts from the beginning of the topic
    consumerProps.put("auto.offset.reset", earliest ? "earliest" : "latest");
    KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(consumerProps);
    consumer.subscribe(Arrays.asList(topic));

    List<String> messages = new ArrayList<>();

    ConsumerRecords<Integer, byte[]> records = consumer.poll(timeout);
    for (ConsumerRecord<Integer, byte[]> record : records) {
      messages.add(new String(record.value()));
    }

    consumer.close();

    return messages;
  }

}
