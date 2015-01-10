/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.rules.TestWatcher;

import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Sink;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class TestUtils
{
  public static class TestInfo extends TestWatcher
  {
    public org.junit.runner.Description desc;

    public String getDir()
    {
      String methodName = desc.getMethodName();
      String className = desc.getClassName();
      return "target/" + className + "/" + methodName;
    }

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.desc = description;
    }
  };

  /**
   * Clone object by serializing and deserializing using Kryo.
   * Note this is different from using {@link Kryo#copy(Object)}, which will attempt to also clone transient fields.
   * @param kryo
   * @param src
   * @return
   * @throws IOException
   */
  public static <T> T clone(Kryo kryo, T src) throws IOException
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output output = new Output(bos);
    kryo.writeObject(output, src);
    output.close();
    Input input = new Input(bos.toByteArray());
    @SuppressWarnings("unchecked")
    Class<T> clazz = (Class<T>)src.getClass();
    return kryo.readObject(input, clazz);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <T> Sink<T> setSink(OutputPort<T> port, Sink<T> sink)
  {
     port.setSink((CollectorTestSink)sink);
     return sink;
  }


}
