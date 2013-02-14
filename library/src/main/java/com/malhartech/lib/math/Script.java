/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.script.*;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class Script extends BaseOperator
{
  protected transient ScriptEngineManager sem = new ScriptEngineManager();
  protected transient ScriptEngine engine = sem.getEngineByName("JavaScript");
  protected String script;
  protected boolean keepContext = true;
  protected boolean isPassThru = true;
  protected SimpleScriptContext context = new SimpleScriptContext();
  protected Object evalResult;

  @InputPortFieldAnnotation(name = "inBindings", optional = true)
  public final transient DefaultInputPort<Map<String, Object>> inBindings = new DefaultInputPort<Map<String, Object>>(this)
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      for (Map.Entry<String, Object> entry: tuple.entrySet()) {
        engine.put(entry.getKey(), entry.getValue());
      }
      Object res;
      try {
        evalResult = engine.eval(script, context);
        if (isPassThru) {
          result.emit(evalResult);
        }
      }
      catch (ScriptException ex) {
        Logger.getLogger(Script.class.getName()).log(Level.SEVERE, null, ex);
      }
      if (isPassThru) {
        outBindings.emit(new HashMap<String, Object>(engine.getBindings(ScriptContext.ENGINE_SCOPE)));
      }
    }

  };
  @OutputPortFieldAnnotation(name = "outBindings", optional = true)
  public final transient DefaultOutputPort<Map<String, Object>> outBindings = new DefaultOutputPort<Map<String, Object>>(this);
  @OutputPortFieldAnnotation(name = "result", optional = true)
  public final transient DefaultOutputPort<Object> result = new DefaultOutputPort<Object>(this);

  public void setEngineByName(String name)
  {
    engine = sem.getEngineByName(name);
  }

  public void setKeepContext(boolean keepContext)
  {
    this.keepContext = keepContext;
  }

  public void setScript(String script)
  {
    this.script = script;
  }

  public void runScript(String script) throws ScriptException
  {
    engine.eval(script, context);
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
      outBindings.emit(new HashMap<String, Object>(this.context.getBindings(ScriptContext.ENGINE_SCOPE)));
    }
    if (!keepContext) {
      this.context = new SimpleScriptContext();
      engine.setContext(this.context);
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    engine.setContext(this.context);
  }

  public void put(String key, Object val)
  {
    this.context.getBindings(ScriptContext.ENGINE_SCOPE).put(key, val);
  }

}
