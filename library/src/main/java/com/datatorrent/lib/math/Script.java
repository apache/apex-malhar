/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.lib.math;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.script.*;

/**
 *
 */
public class Script extends BaseOperator
{
  public enum Type
  {
    EVAL, INVOKE
  };

  protected transient ScriptEngineManager sem = new ScriptEngineManager();
  protected transient ScriptEngine engine = sem.getEngineByName("JavaScript");
  protected String scriptOrFunction;
  protected Type type = Type.EVAL;
  protected boolean keepContext = true;
  protected boolean isPassThru = true;
  protected transient SimpleScriptContext scriptContext = new SimpleScriptContext();
  protected SimpleBindings scriptBindings = new SimpleBindings();
  protected ArrayList<String> setupScripts = new ArrayList<String>();
  protected Object evalResult;

  @InputPortFieldAnnotation(name = "inBindings", optional = true)
  public final transient DefaultInputPort<Map<String, Object>> inBindings = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      for (Map.Entry<String, Object> entry: tuple.entrySet()) {
        engine.put(entry.getKey(), entry.getValue());
      }
      Object res;
      try {
        switch (type) {
          case EVAL:
            evalResult = engine.eval(scriptOrFunction, scriptContext);
            break;
          case INVOKE:
            evalResult = ((Invocable)engine).invokeFunction(scriptOrFunction);
            break;
        }

        if (isPassThru) {
          result.emit(evalResult);
        }
      }
      catch (Exception ex) {
        throw new RuntimeException(ex);
      }

      if (isPassThru) {
        outBindings.emit(new HashMap<String, Object>(engine.getBindings(ScriptContext.ENGINE_SCOPE)));
      }
    }

  };
  @OutputPortFieldAnnotation(name = "outBindings", optional = true)
  public final transient DefaultOutputPort<Map<String, Object>> outBindings = new DefaultOutputPort<Map<String, Object>>();
  @OutputPortFieldAnnotation(name = "result", optional = true)
  public final transient DefaultOutputPort<Object> result = new DefaultOutputPort<Object>();

  public void setEngineByName(String name)
  {
    engine = sem.getEngineByName(name);
  }

  public void setKeepContext(boolean keepContext)
  {
    this.keepContext = keepContext;
  }

  public void setEval(String script)
  {
    this.type = Type.EVAL;
    this.scriptOrFunction = script;
  }

  public void setInvoke(String functionName)
  {
    this.type = Type.INVOKE;
    this.scriptOrFunction = functionName;
  }

  public void addSetupScript(String script)
  {
    setupScripts.add(script);
  }

  public void setPassThru(boolean isPassThru)
  {
    this.isPassThru = isPassThru;
  }

  @Override
  public void endWindow()
  {
    if (!isPassThru) {
      result.emit(evalResult);
      outBindings.emit(new HashMap<String, Object>(this.scriptContext.getBindings(ScriptContext.ENGINE_SCOPE)));
    }
    if (!keepContext) {
      this.scriptContext = new SimpleScriptContext();
      engine.setContext(this.scriptContext);
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    this.scriptContext.setBindings(scriptBindings, ScriptContext.ENGINE_SCOPE);
    engine.setContext(this.scriptContext);
    try {
      for (String s: setupScripts) {
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
