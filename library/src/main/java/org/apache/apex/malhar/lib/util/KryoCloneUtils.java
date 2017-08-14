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
package org.apache.apex.malhar.lib.util;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Array;

import org.apache.commons.io.IOUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 *
 * A Kryo Clone Util class that clone object by using Kryo serializer and deserializer
 * The class has static method that are can be used directly to clone one object
 * Or it can be used as util instance to clone as many objects as you need from the one source object
 *
 * @since 3.4.0
 */
public class KryoCloneUtils<T>
{

  /**
   * Reusable Kryo object as deserializer
   */
  private final Kryo kryo;

  /**
   * Reusable binary data for object that would be deserialized from
   */
  private final byte[] bin;

  /**
   * The class of the object
   */
  private final Class<T> clazz;

  @SuppressWarnings("unchecked")
  private KryoCloneUtils(Kryo kryo, T t)
  {
    this.kryo = kryo;
    ByteArrayOutputStream bos = null;
    Output output = null;
    try {
      bos = new ByteArrayOutputStream();
      output = new Output(bos);
      kryo.writeObject(output, t);
      output.close();
      bin = bos.toByteArray();
    } finally {
      IOUtils.closeQuietly(output);
      IOUtils.closeQuietly(bos);
    }
    clazz = (Class<T>)t.getClass();
    kryo.setClassLoader(clazz.getClassLoader());
  }

  /**
   * Clone from the binary data of the source object
   * @return T
   */
  public T getClone()
  {
    try (Input input = new Input(bin)) {
      return kryo.readObject(input, clazz);
    }
  }

  /**
   * Clone array of objects from source object
   * @param num size of the return array
   * @return array of T
   */
  @SuppressWarnings("unchecked")
  public T[] getClones(int num)
  {
    T[] ts = (T[])Array.newInstance(clazz, num);
    try (Input input = new Input(bin)) {
      for (int i = 0; i < ts.length; i++) {
        input.rewind();
        ts[i] = kryo.readObject(input, clazz);
      }
    }
    return ts;
  }



  /**
   * Clone object by serializing and deserializing using Kryo.
   * Note this is different from using {@link Kryo#copy(Object)}, which will attempt to also clone transient fields.
   *
   * @param kryo kryo object used to clone objects
   * @param src src object that copy from
   * @return
   */
  @SuppressWarnings("unchecked")
  public static <SRC> SRC cloneObject(Kryo kryo, SRC src)
  {
    kryo.setClassLoader(src.getClass().getClassLoader());
    ByteArrayOutputStream bos = null;
    Output output;
    Input input = null;
    try {
      bos = new ByteArrayOutputStream();
      output = new Output(bos);
      kryo.writeObject(output, src);
      output.close();
      input = new Input(bos.toByteArray());
      return (SRC)kryo.readObject(input, src.getClass());
    } finally {
      IOUtils.closeQuietly(input);
      IOUtils.closeQuietly(bos);
    }
  }


  /**
   * Clone object by serializing and deserializing using default Kryo.
   * Note this is different from using {@link Kryo#copy(Object)}, which will attempt to also clone transient fields.
   *
   * @param src src object that copy from
   * @return
   */
  public static <SRC> SRC cloneObject(SRC src)
  {
    return cloneObject(new Kryo(), src);
  }

  /**
   * Factory function to return CloneUtils object
   * @param template
   * @param <SRC>
   * @return
   */
  public static <SRC> KryoCloneUtils<SRC> createCloneUtils(SRC template)
  {
    return createCloneUtils(new Kryo(), template);
  }

  /**
   * Factory function to return CloneUtils object with customized Kryo
   * @param kryo
   * @param template
   * @param <SRC>
   * @return
   */
  public static <SRC> KryoCloneUtils<SRC> createCloneUtils(Kryo kryo, SRC template)
  {
    return new KryoCloneUtils<>(kryo, template);
  }

}
