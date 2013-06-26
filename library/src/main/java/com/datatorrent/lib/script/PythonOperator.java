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
 * limitations under the License.
 */
package com.datatorrent.lib.script;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import org.python.core.PyObject;
import org.python.util.PythonInterpreter;
/**
 * Operator to execute python script on tuples 
 *
 */
public class PythonOperator extends  ScriptBaseOperator
{
	public final transient DefaultInputPort<Map<String, Object>> inBindings = 
			new DefaultInputPort<Map<String, Object>>()
			{
				@Override
				public void process(Map<String, Object> tuple)
				{
					tuple = executescriptCode(tuple);
					PythonOperator.this.setTuple(tuple);
				}

				private Map<String, Object> executescriptCode(Map<String, Object> tuple)
				{
					// no code to execute
					if ((PythonOperator.this.scriptCode == null)||(PythonOperator.this.scriptCode.length() == 0))
					{
						return tuple;
					}
					
					// execute python script
					PythonInterpreter interp = new PythonInterpreter();
					interp.exec("import sys");
					for(Entry<String, Object> entry : tuple.entrySet())
					{
						interp.set(entry.getKey(), entry.getValue());
					}
					interp.exec(PythonOperator.this.scriptCode);
					
					// return result 
					HashMap<String, Object> result = new HashMap<String, Object>();
					for(Entry<String, Object> entry : tuple.entrySet())
					{
						PyObject value = interp.get(entry.getKey());
						result.put(entry.getKey(), value.__tojava__(entry.getValue().getClass()));
					}
					return result;
				}
			};
}
