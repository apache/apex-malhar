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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import com.datatorrent.api.DefaultInputPort;

/**
 * Operator to execute python script on tuples
 *
 * @since 0.3.3
 */
public class PythonOperator extends ScriptBaseOperator
{
	/**
	 * Python script interpretor.
	 */
	private PythonInterpreter interp = new PythonInterpreter();
	
	// Constructor 
	public PythonOperator() {
		interp.exec("import sys");
	}
	
	/**
	 * Input port, variable/value map.
	 */
	public final transient DefaultInputPort<Map<String, Object>> inBindings = new DefaultInputPort<Map<String, Object>>()
	{
		/**
		 * Execute python code with variable value map.
		 */
		@Override
		public void process(Map<String, Object> tuple)
		{
			tuple = executescriptCode(tuple);
			PythonOperator.this.setTuple(tuple);
		}

		/**
		 * Execute python code with variable value map.
		 */
		private Map<String, Object> executescriptCode(Map<String, Object> tuple)
		{
			// no code to execute
			if ((PythonOperator.this.scriptCode == null)
					|| (PythonOperator.this.scriptCode.length() == 0)) {
				return tuple;
			}

			// execute python script
			for (Entry<String, Object> entry : tuple.entrySet()) {
				interp.set(entry.getKey(), entry.getValue());
			}
			interp.exec(PythonOperator.this.scriptCode);

			// return result
			HashMap<String, Object> result = new HashMap<String, Object>();
			for (Entry<String, Object> entry : tuple.entrySet()) {
				PyObject value = interp.get(entry.getKey());
				result.put(entry.getKey(),
						value.__tojava__(entry.getValue().getClass()));
			}
			return result;
		}
	};
}
