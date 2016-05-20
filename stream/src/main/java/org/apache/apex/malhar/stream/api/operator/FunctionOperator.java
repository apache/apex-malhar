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
package org.apache.apex.malhar.stream.api.operator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.commons.io.IOUtils;

import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.ClassReader;
import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.ClassWriter;
import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.Opcodes;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * Operators that wrap the functions
 */
public class FunctionOperator<OUT, FUNCTION extends Function> implements Operator
{
  private byte[] annonymousFunctionClass;

  protected transient FUNCTION statelessF;

  protected FUNCTION statefulF;

  protected boolean stateful = false;

  protected boolean isAnnonymous = false;

  public final transient DefaultOutputPort<OUT> output = new DefaultOutputPort<>();

  public FunctionOperator(FUNCTION f)
  {
    isAnnonymous = f.getClass().isAnonymousClass();
    if (isAnnonymous) {
      annonymousFunctionClass = functionClassData(f);
    } else if (f instanceof Function.Stateful) {
      statelessF = f;
    } else {
      statefulF = f;
      stateful = true;
    }
  }

  private byte[] functionClassData(Function f)
  {
    Class<? extends Function> classT = f.getClass();

    byte[] classBytes = null;
    byte[] classNameBytes = null;
    String className = classT.getName();
    try {
      classNameBytes = className.replace('.', '/').getBytes();
      classBytes = IOUtils.toByteArray(classT.getClassLoader().getResourceAsStream(className.replace('.', '/') + ".class"));
      int cursor = 0;
      for (int j = 0; j < classBytes.length; j++) {
        if (classBytes[j] != classNameBytes[cursor]) {
          cursor = 0;
        } else {
          cursor++;
        }

        if (cursor == classNameBytes.length) {
          for (int p = 0; p < classNameBytes.length; p++) {
            if (classBytes[j - p] == '$') {
              classBytes[j - p] = '_';
            }
          }
          cursor = 0;
        }
      }
      ClassReader cr = new ClassReader(new ByteArrayInputStream(classBytes));
      ClassWriter cw = new ClassWriter(0);
      AnnonymousClassModifier annonymousClassModifier = new AnnonymousClassModifier(Opcodes.ASM4, cw);
      cr.accept(annonymousClassModifier, 0);
      classBytes = cw.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    int dataLength = classNameBytes.length + 4 + 4;

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(dataLength);
    DataOutputStream output = new DataOutputStream(byteArrayOutputStream);

    try {
      output.writeInt(classNameBytes.length);
      output.write(className.replace('$', '_').getBytes());
      output.writeInt(classBytes.length);
      output.write(classBytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        output.flush();
        output.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    return byteArrayOutputStream.toByteArray();

  }

  /**
   * Default constructor to make kryo happy
   */
  public FunctionOperator()
  {

  }

  @Override
  public void beginWindow(long l)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    readFunction();
  }


  @SuppressWarnings("unchecked")
  private void readFunction()
  {
    try {
      if (statelessF != null || statefulF != null) {
        return;
      }
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(annonymousFunctionClass));
      byte[] classNameBytes = new byte[input.readInt()];
      input.read(classNameBytes);
      String className = new String(classNameBytes);
      byte[] classData = new byte[input.readInt()];
      input.read(classData);
      Map<String, byte[]> classBin = new HashMap<>();
      classBin.put(className, classData);
      ByteArrayClassLoader byteArrayClassLoader = new ByteArrayClassLoader(classBin, Thread.currentThread().getContextClassLoader());
      statelessF = ((Class<FUNCTION>)byteArrayClassLoader.findClass(className)).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown()
  {

  }

  public FUNCTION getFunction()
  {
    readFunction();
    if (stateful) {
      return statefulF;
    } else {
      return statelessF;
    }
  }

  public FUNCTION getStatelessF()
  {
    return statelessF;
  }

  public void setStatelessF(FUNCTION statelessF)
  {
    this.statelessF = statelessF;
  }

  public FUNCTION getStatefulF()
  {
    return statefulF;
  }

  public void setStatefulF(FUNCTION statefulF)
  {
    this.statefulF = statefulF;
  }

  public boolean isStateful()
  {
    return stateful;
  }

  public void setStateful(boolean stateful)
  {
    this.stateful = stateful;
  }

  public boolean isAnnonymous()
  {
    return isAnnonymous;
  }

  public void setIsAnnonymous(boolean isAnnonymous)
  {
    this.isAnnonymous = isAnnonymous;
  }

  public static class MapFunctionOperator<IN, OUT> extends FunctionOperator<OUT, Function.MapFunction<IN, OUT>>
  {

    public MapFunctionOperator()
    {

    }

    public final transient DefaultInputPort<IN> input = new DefaultInputPort<IN>()
    {
      @Override
      public void process(IN t)
      {
        Function.MapFunction<IN, OUT> f = getFunction();
        output.emit(f.f(t));
      }
    };

    public MapFunctionOperator(Function.MapFunction<IN, OUT> f)
    {
      super(f);
    }
  }

  public static class FlatMapFunctionOperator<IN, OUT> extends FunctionOperator<OUT, Function.FlatMapFunction<IN, OUT>>
  {

    public FlatMapFunctionOperator()
    {

    }

    public final transient DefaultInputPort<IN> input = new DefaultInputPort<IN>()
    {
      @Override
      public void process(IN t)
      {
        Function.FlatMapFunction<IN, OUT> f = getFunction();
        for (OUT out : f.f(t)) {
          output.emit(out);
        }
      }
    };

    public FlatMapFunctionOperator(Function.FlatMapFunction<IN, OUT> f)
    {
      super(f);
    }
  }

  public static class FoldFunctionOperator<IN, OUT> extends FunctionOperator<OUT, Function.FoldFunction<IN, OUT>>
  {

    public FoldFunctionOperator()
    {

    }

    @NotNull
    private OUT foldVal;

    public final transient DefaultInputPort<IN> input = new DefaultInputPort<IN>()
    {
      @Override
      public void process(IN t)
      {
        Function.FoldFunction<IN, OUT> f = getFunction();
        // fold the value
        foldVal = f.fold(t, foldVal);
        output.emit(foldVal);
      }
    };

    public FoldFunctionOperator(Function.FoldFunction<IN, OUT> f, OUT initialVal)
    {
      super(f);
      this.foldVal = initialVal;
    }
  }

  public static class ReduceFunctionOperator<IN> extends FunctionOperator<IN, Function.ReduceFunction<IN>>
  {

    public ReduceFunctionOperator()
    {

    }

    @NotNull
    private IN reducedVal;

    public final transient DefaultInputPort<IN> input = new DefaultInputPort<IN>()
    {
      @Override
      public void process(IN t)
      {
        Function.ReduceFunction<IN> f = getFunction();
        // fold the value
        if (reducedVal == null) {
          reducedVal = t;
          return;
        }
        reducedVal = f.reduce(t, reducedVal);
        output.emit(reducedVal);
      }
    };

    public ReduceFunctionOperator(Function.ReduceFunction<IN> f)
    {
      super(f);
    }
  }

  public static class FilterFunctionOperator<IN> extends FunctionOperator<IN, Function.FilterFunction<IN>>
  {

    public FilterFunctionOperator()
    {

    }

    public final transient DefaultInputPort<IN> input = new DefaultInputPort<IN>()
    {
      @Override
      public void process(IN t)
      {
        Function.FilterFunction<IN> f = getFunction();
        // fold the value
        if (f.f(t)) {
          output.emit(t);
        }
      }
    };

    public FilterFunctionOperator(Function.FilterFunction<IN> f)
    {
      super(f);
    }

  }
}
