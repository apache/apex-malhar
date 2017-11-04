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
package org.apache.apex.malhar.lib.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.esotericsoftware.kryo.util.ObjectMap;

/**
 * Extension of {@JavaSerializer} for consistent class loading behavior.
 *
 * <p>This class can be removed after upgrade to a Kryo release > 4.0
 * see https://github.com/EsotericSoftware/kryo/commit/19a6b5edee7125fbaf54c64084a8d0e13509920b
 *
 * @since 3.8.0
 */
public class KryoJavaSerializer extends JavaSerializer
{
  @Override
  public Object read(Kryo kryo, Input input, Class type)
  {
    try {
      ObjectMap graphContext = kryo.getGraphContext();
      ObjectInputStream objectStream = (ObjectInputStream)graphContext.get(this);
      if (objectStream == null) {
        objectStream = new ObjectInputStreamWithKryoClassLoader(input, kryo);
        graphContext.put(this, objectStream);
      }
      return objectStream.readObject();
    } catch (Exception ex) {
      throw new KryoException("Error during Java deserialization.", ex);
    }
  }

  private static class ObjectInputStreamWithKryoClassLoader extends ObjectInputStream
  {
    private final ClassLoader loader;

    ObjectInputStreamWithKryoClassLoader(InputStream in, Kryo kryo) throws IOException
    {
      super(in);
      this.loader = kryo.getClassLoader();
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc)
    {
      try {
        return Class.forName(desc.getName(), false, loader);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Class not found: " + desc.getName(), e);
      }
    }
  }
}
