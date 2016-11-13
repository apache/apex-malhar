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
public class GenericSerde<T> implements Serde<T>
{
  private transient ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>()
  {
    @Override
    public Kryo get()
    {
      return new Kryo();
    }
  };

  private final Class<? extends T> clazz;

  public GenericSerde()
  {
    this.clazz = null;
  }

  public GenericSerde(Class<? extends T> clazz)
  {
    this.clazz = clazz;
  }

  @Override
  public void serialize(T object, Output output)
  {
    Kryo kryo = kryos.get();
    if (clazz == null) {
      kryo.writeClassAndObject(output, object);
    } else {
      kryo.writeObject(output, object);
    }
  }

  @Override
  public T deserialize(Input input)
  {
    T object;
    Kryo kryo = kryos.get();
    if (clazz == null) {
      object = (T)kryo.readClassAndObject(input);
    } else {
      object = kryo.readObject(input, clazz);
    }
    return object;
  }
}
