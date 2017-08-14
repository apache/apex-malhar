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
package org.apache.apex.examples.mroperator;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.KeyHashValPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * <p>OutputCollectorImpl class.</p>
 *
 * @since 0.9.0
 */
@SuppressWarnings("unchecked")
public class OutputCollectorImpl<K extends Object, V extends Object> implements OutputCollector<K, V>
{
  private static final Logger logger = LoggerFactory.getLogger(OutputCollectorImpl.class);

  private List<KeyHashValPair<K, V>> list = new ArrayList<KeyHashValPair<K, V>>();

  public List<KeyHashValPair<K, V>> getList()
  {
    return list;
  }

  private transient SerializationFactory serializationFactory;
  private transient Configuration conf = null;

  public OutputCollectorImpl()
  {
    conf = new Configuration();
    serializationFactory = new SerializationFactory(conf);

  }

  private <T> T cloneObj(T t) throws IOException
  {
    Serializer<T> keySerializer;
    Class<T> keyClass;
    PipedInputStream pis = new PipedInputStream();
    PipedOutputStream pos = new PipedOutputStream(pis);
    keyClass = (Class<T>)t.getClass();
    keySerializer = serializationFactory.getSerializer(keyClass);
    keySerializer.open(pos);
    keySerializer.serialize(t);
    Deserializer<T> keyDesiralizer = serializationFactory.getDeserializer(keyClass);
    keyDesiralizer.open(pis);
    T clonedArg0 = keyDesiralizer.deserialize(null);
    pos.close();
    pis.close();
    keySerializer.close();
    keyDesiralizer.close();
    return clonedArg0;

  }

  @Override
  public void collect(K arg0, V arg1) throws IOException
  {
    if (conf == null) {
      conf = new Configuration();
      serializationFactory = new SerializationFactory(conf);
    }
    list.add(new KeyHashValPair<K, V>(cloneObj(arg0), cloneObj(arg1)));
  }
}
