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
package org.apache.apex.malhar.lib.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.netlet.util.Slice;

/**
 * This codec is used for serializing the objects of class which are Kryo serializable.
 * It is needed when custom static partitioning is required.
 *
 * @param <T> Type of the object which gets serialized/deserialized using this codec.
 * @since 0.9.0
 */
public class KryoSerializableStreamCodec<T> implements StreamCodec<T>, Serializable
{
  protected transient Kryo kryo;

  public KryoSerializableStreamCodec()
  {
    this.kryo = new Kryo();
    this.kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
  {
    in.defaultReadObject();
    this.kryo = new Kryo();
    this.kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
  }

  /**
   * Registers a class with kryo. If the class of the tuple and its fields are registered then kryo serialization is
   * more efficient.
   *
   * @param clazz class to register with Kryo.
   */
  public void register(Class<?> clazz)
  {
    this.kryo.register(clazz);
  }

  /**
   * Register a class with specified id.
   *
   * @param clazz class to register with Kryo.
   * @param id    int ID of the class.
   */
  public void register(Class<?> clazz, int id)
  {
    Preconditions.checkArgument(id > 0, "invalid id");
    this.kryo.register(clazz, id);
  }

  @Override
  public Object fromByteArray(Slice fragment)
  {
    ByteArrayInputStream is = new ByteArrayInputStream(fragment.buffer, fragment.offset, fragment.length);
    Input input = new Input(is);
    return kryo.readClassAndObject(input);
  }

  @Override
  public Slice toByteArray(T info)
  {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Output output = new Output(os);

    kryo.writeClassAndObject(output, info);
    output.flush();
    return new Slice(os.toByteArray(), 0, os.toByteArray().length);
  }

  @Override
  public int getPartition(T t)
  {
    return t.hashCode();
  }

  private static final long serialVersionUID = 201411031402L;
}
