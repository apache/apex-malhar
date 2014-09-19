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
package com.datatorrent.contrib.jython;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.script.ScriptOperator;
import org.python.core.*;
import org.python.util.PythonInterpreter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An operator that executes a python script and passes the input as bindings.
 * <p></p>
 * @displayName Python
 * @category languages
 * @tags python, script
 *
 * @since 0.3.3
 */
public class PythonOperator extends ScriptOperator
{
  /**
   * Python script interpretor.
   */
  private PythonInterpreter interp = new PythonInterpreter();
  private PyObject evalResult;
  private PyCode code;

  // Constructor
  public PythonOperator()
  {
    interp.exec("import sys");
  }

  @Override
  public void setup(OperatorContext context)
  {
    for (String s : setupScripts) {
      interp.exec(s);
    }
    code = interp.compile(script);
  }

  @Override
  public void process(Map<String, Object> tuple)
  {
    for (Map.Entry<String, Object> entry : tuple.entrySet()) {
      interp.set(entry.getKey(), entry.getValue());
    }
    evalResult = interp.eval(code);
    if (isPassThru) {
      if (result.isConnected()) {
        result.emit(evalResult);
      }
      if (outBindings.isConnected()) {
        outBindings.emit(new HashMap<String, Object>(getBindings()));
      }
    }
  }

  @Override
  public void endWindow()
  {
    if (!isPassThru) {
      result.emit(evalResult);
      outBindings.emit(new HashMap<String, Object>(getBindings()));
    }
  }

  @Override
  public Map<String, Object> getBindings()
  {
    Map<String, Object> bindings = new HashMap<String, Object>();
    PyStringMap keyValueMap = (PyStringMap)interp.getLocals();
    PyIterator keyValueSet = (PyIterator)keyValueMap.iteritems();
    for (Object temp : keyValueSet) {
      PyTuple tempEntry = (PyTuple)temp;
      Iterator<PyObject> iter = tempEntry.iterator();
      bindings.put((String)iter.next().__tojava__(String.class), iter.next());
    }
    return bindings;
  }

}
