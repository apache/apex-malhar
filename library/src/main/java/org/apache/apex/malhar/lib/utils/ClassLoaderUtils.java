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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;

@InterfaceStability.Evolving
/**
 * @since 3.6.0
 */
public class ClassLoaderUtils
{
  /**
   * Given the class name it reads and loads the class from the input stream.
   *
   * @param fqcn        fully qualified class name.
   * @param inputStream stream from which class is read.
   * @return loaded class
   * @throws IOException
   */
  public static Class<?> readBeanClass(String fqcn, FSDataInputStream inputStream) throws IOException
  {
    byte[] bytes = IOUtils.toByteArray(inputStream);
    inputStream.close();
    return new ByteArrayClassLoader().defineClass(fqcn, bytes);
  }

  /**
   * Given the class name it reads and loads the class from given byte array.
   *
   * @param fqcn       fully qualified class name.
   * @param inputClass byte[] from which class is read.
   * @return loaded class
   * @throws IOException
   */
  public static Class<?> readBeanClass(String fqcn, byte[] inputClass) throws IOException
  {
    return new ByteArrayClassLoader().defineClass(fqcn, inputClass);
  }

  /**
   * Byte Array class loader for loading class from byte[]
   */
  public static class ByteArrayClassLoader extends ClassLoader
  {
    Class<?> defineClass(String name, byte[] ba)
    {
      return defineClass(name, ba, 0, ba.length);
    }
  }
}
