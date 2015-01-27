/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.script;

import com.datatorrent.api.Context.OperatorContext;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

import java.util.HashMap;
import java.util.Map;
import javax.script.*;

/**
 * An implementation of ScriptOperator that executes JavaScript on tuples input for Map &lt;String, Object&gt;.
 *
 * <p>
 * Key is name of variable used in script code. Proper map values must be provided
 * by up stream operators.
 *
 * <b> Sample Usage Code : </b>
 *
 * <pre>
 * // Add random integer generator operator
 * SingleKeyValMap rand = dag.addOperator(&quot;rand&quot;, SingleKeyValMap.class);
 *
 * Script script = dag.addOperator(&quot;script&quot;, Script.class);
 * // script.setEval(&quot;val = val*val;&quot;);
 * script.addSetupScript(&quot;function square() { return val*val;}&quot;);
 * script.setInvoke(&quot;square&quot;);
 * dag.addStream(&quot;evalstream&quot;, rand.outport, script.inBindings);
 *
 * // Connect to output console operator
 * ConsoleOutputOperator console = dag.addOperator(&quot;console&quot;,
 * 		new ConsoleOutputOperator());
 * dag.addStream(&quot;rand_console&quot;, script.result, console.input);
 *
 * </pre>
 *
 * <b> Sample Input Operator(emit)</b>
 *
 * <pre>
 *  	.
 * 		.
 * 		public void emitTuples() {
 * 			HashMap<String, Object> map = new HashMap<String, Object>();
 * 			map.put("val", random.nextInt());
 * 			outport.emit(map);
 * 			.
 * 			.
 * 		}
 * 		.
 * 		.
 * </pre>
 *
 * This operator does not checkpoint interpreted functions in the variable bindings because they are not serializable
 * Use setupScript() to define functions, and do NOT define or assign functions to variables at run time
 * @displayName Java Script
 * @category Scripting
 * @tags script operator, map, string
 * @since 0.3.2
 */
public class JavaScriptOperator extends ScriptOperator
{
  public enum Type
  {
    EVAL, INVOKE
  };

  public static class BindingsSerializer<T> extends FieldSerializer<T>
  {
    @SuppressWarnings("rawtypes")
    public BindingsSerializer(Kryo kryo, Class<T> clazz)
    {
      super(kryo, clazz);
      try {
        Class<?> interpretedFunctionClass = Class.forName("sun.org.mozilla.javascript.internal.InterpretedFunction");

        kryo.register(interpretedFunctionClass,
                      new FieldSerializer(kryo, interpretedFunctionClass)
        {
          @Override
          protected Object create(Kryo kryo, Input input, Class type)
          {
            return new HashMap<String, Object>(); // hack to bypass unserializable interpreted function object
          }

        });
      }
      catch (ClassNotFoundException ex) {
        // ignore
      }
    }

  }

  @DefaultSerializer(value = BindingsSerializer.class)
  protected static class MyBindings extends SimpleBindings
  {
    public MyBindings(){
      
    }
  }

  protected transient ScriptEngineManager sem = new ScriptEngineManager();
  protected transient ScriptEngine engine = sem.getEngineByName("JavaScript");
  protected Type type = Type.EVAL;
  protected transient SimpleScriptContext scriptContext = new SimpleScriptContext();
  protected MyBindings scriptBindings = new MyBindings();
  protected Object evalResult;

  @Override
  public void process(Map<String, Object> tuple)
  {
    for (Map.Entry<String, Object> entry : tuple.entrySet()) {
      engine.put(entry.getKey(), entry.getValue());
    }
    try {
      switch (type) {
        case EVAL:
          evalResult = engine.eval(JavaScriptOperator.this.script, scriptContext);
          break;
        case INVOKE:
          evalResult = ((Invocable)engine).invokeFunction(script);
          break;
      }

      if (isPassThru && result.isConnected()) {
        result.emit(evalResult);
      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    if (isPassThru && outBindings.isConnected()) {
      outBindings.emit(getBindings());
    }
  }

  @Override
  public Map<String, Object> getBindings()
  {
    return new HashMap<String, Object>(engine.getBindings(ScriptContext.ENGINE_SCOPE));
  }

  public void setEngineByName(String name)
  {
    engine = sem.getEngineByName(name);
  }


  public void setEval(String script)
  {
    this.type = Type.EVAL;
    this.script = script;
  }

  public void setInvoke(String functionName)
  {
    this.type = Type.INVOKE;
    this.script = functionName;
  }

  @Override
  public void endWindow()
  {
    if (!isPassThru) {
      result.emit(evalResult);
      outBindings.emit(getBindings());
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    this.scriptContext.setBindings(scriptBindings, ScriptContext.ENGINE_SCOPE);
    engine.setContext(this.scriptContext);
    try {
      for (String s : setupScripts) {
        engine.eval(s, this.scriptContext);
      }
    }
    catch (ScriptException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void put(String key, Object val)
  {
    scriptBindings.put(key, val);
  }

}
