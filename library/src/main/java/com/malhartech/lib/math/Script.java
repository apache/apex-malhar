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
  protected boolean keepBindings = false;
  protected boolean isPassThru = true;
  protected SimpleBindings bindings = new SimpleBindings();
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
        evalResult = engine.eval(script);
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

  public void setKeepBindings(boolean keepBindings)
  {
    this.keepBindings = keepBindings;
  }

  public void setScript(String script)
  {
    this.script = script;
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
      outBindings.emit(new HashMap<String, Object>(engine.getBindings(ScriptContext.ENGINE_SCOPE)));
    }
    if (!keepBindings) {
      engine.getBindings(ScriptContext.ENGINE_SCOPE).clear();
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    engine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
  }

  public void put(String key, Object val)
  {
    bindings.put(key, val);
  }

}
