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
package org.apache.apex.malhar.lib.utils.serde;

import java.io.ByteArrayOutputStream;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Generic serde using Kryo serialization. Note that while this is convenient, it may not be desirable because
 * using Kryo makes the object being serialized rigid, meaning you won't be able to make backward compatible or
 * incompatible changes to the class being serialized.
 *
 * @param <T> The type being serialized
 */
@InterfaceStability.Evolving
public class KryoSerde<T> implements Serde<T>
{
  // Setup ThreadLocal of Kryo instances
  private static final ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>()
  {
    @Override
    protected Kryo initialValue()
    {
      Kryo kryo = new Kryo();
      // configure kryo instance, customize settings
      return kryo;
    }
  };

  private final Class<? extends T> clazz;

  public KryoSerde()
  {
    this.clazz = null;
  }

  public KryoSerde(Class<? extends T> clazz)
  {
    this.clazz = clazz;
  }

  /**
   * Current implementation writes to the temporary ByteArrayOutputStream and then copy
   * TODO: let SerializationBuffer support kyro.
   */
  @Override
  public void serialize(T object, SerializationBuffer buffer)
  {
    Kryo kryo = kryos.get();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    Output output = new Output(stream);
    if (clazz == null) {
      kryo.writeClassAndObject(output, object);
    } else {
      kryo.writeObject(output, object);
    }
    buffer.write(output.toBytes());
  }

  @Override
  public T deserialize(byte[] buffer, MutableInt offset, int length)
  {
    Kryo kryo = kryos.get();
    Input input = new Input(buffer, offset.intValue(), length);
    T object;
    if (clazz == null) {
      object = (T)kryo.readClassAndObject(input);
    } else {
      object = kryo.readObject(input, clazz);
    }
    offset.add(length);
    return object;
  }
}
