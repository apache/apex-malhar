/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.util;

import java.util.Map;

import javax.script.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;

/**
 * This operator consumes tuples.&nbsp;If the tuples satisfy a specified Java Script filtering function, then they are emitted.
 * <p></p>
 * @displayName Java Script Filter
 * @category Algorithmic
 * @tags filter
 * @since 0.3.4
 */
public class JavaScriptFilterOperator extends FilterOperator
{
  protected transient ScriptEngineManager sem = new ScriptEngineManager();
  protected transient ScriptEngine engine = sem.getEngineByName("JavaScript");
  protected transient SimpleScriptContext scriptContext = new SimpleScriptContext();
  protected transient SimpleBindings scriptBindings = new SimpleBindings();
  protected String functionName;
  protected String setupScript;
  private static final Logger LOG = LoggerFactory.getLogger(JavaScriptFilterOperator.class);

  public String getFunctionName()
  {
    return functionName;
  }

  /**
   * Enter the JavaScript to run against every input.
   * @param script
   */
  public void setFunctionName(String script)
  {
    this.functionName = script;
  }

  public String getSetupScript()
  {
    return setupScript;
  }

  /**
   * Enter the JavaScript to setup the environment.
   * @param script
   */
  
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
      LOG.error("script \"{}\" has error", setupScript);
      throw new RuntimeException(ex);
    }
  }

  @Override
  public boolean satisfiesFilter(Object tuple)
  {
    if (tuple instanceof Map) {
      Map<String, Object> map = (Map<String, Object>)tuple;
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        LOG.debug("Putting {} = {}", entry.getKey(), entry.getValue());
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
        LOG.warn("The script result (type: {}) cannot be converted to boolean. Returning false.", result == null ? "null" : result.getClass().getName());
        return false;
      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
