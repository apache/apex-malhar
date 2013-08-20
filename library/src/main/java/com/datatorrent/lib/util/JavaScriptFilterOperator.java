/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import com.datatorrent.api.Context.OperatorContext;
import java.util.Map;
import javax.script.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class JavaScriptFilterOperator extends FilterOperator
{
  protected transient ScriptEngineManager sem = new ScriptEngineManager();
  protected transient ScriptEngine engine = sem.getEngineByName("JavaScript");
  protected transient SimpleScriptContext scriptContext = new SimpleScriptContext();
  protected SimpleBindings scriptBindings = new SimpleBindings();
  protected String functionName;
  protected String setupScript;
  private static final Logger LOG = LoggerFactory.getLogger(JavaScriptFilterOperator.class);

  public String getFunctionName()
  {
    return functionName;
  }

  public void setFunctionName(String script)
  {
    this.functionName = script;
  }

  public void setSetupScript(String script)
  {
    setupScript = script;
  }

  @Override
  public void setup(OperatorContext context)
  {
    this.scriptContext.setBindings(scriptBindings, ScriptContext.ENGINE_SCOPE);
    engine.setContext(this.scriptContext);
    try {
      if (setupScript != null) {
        engine.eval(setupScript, this.scriptContext);
      }
    }
    catch (ScriptException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public boolean satisfiesFilter(Object tuple)
  {
    if (tuple instanceof Map) {
      Map<String, Object> map = (Map<String, Object>)tuple;
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        engine.put(entry.getKey(), entry.getValue());
      }
    }
    try {
      Object result = ((Invocable)engine).invokeFunction(functionName);
      if (result instanceof Boolean) {
        return (Boolean)result;
      }
      else if (result instanceof Integer) {
        return ((Integer)result) != 0;
      }
      else if (result instanceof Long) {
        return ((Long)result) != 0;
      }
      else if (result instanceof String) {
        return Boolean.getBoolean((String)result);
      }
      else {
        LOG.warn("The script result cannot be converted to boolean. Returning false.");
        return false;
      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
