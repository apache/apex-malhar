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

import bsh.EvalError;
import bsh.Interpreter;

import com.datatorrent.api.DefaultInputPort;

/**
 * Operator to execute bash script on tuples.
 */
public class BashOperator extends ScriptBaseOperator
{
	/**
	 * Input port, variable value map.
	 */
	public final transient DefaultInputPort<Map<String, Object>> inBindings = new DefaultInputPort<Map<String, Object>>()
	{
		/**
		 * Execute bash script on incoming tuple.
		 */
		@Override
		public void process(Map<String, Object> tuple)
		{
			tuple = executeCode(tuple);
			BashOperator.this.setTuple(tuple);
		}

		/**
		 * Append variable initialize code to script and execute script in bash
		 * interpretor.
		 * 
		 * @param tuple
		 *          variable initial value map.
		 * @return variable final value map
		 */
		private Map<String, Object> executeCode(Map<String, Object> tuple)
		{
			if ((scriptCode == null) || (scriptCode.length() == 0))
				return tuple;
			Interpreter interp = new Interpreter();
			try {
				// execute script in bash interpretor
				StringBuilder builder = new StringBuilder();
				for (Entry<String, Object> entry : tuple.entrySet()) {
					String key = entry.getKey();
					builder.append(key.replace('`', ' ')).append(" = ");
					if (entry.getValue() instanceof String) {
						builder.append("\"");
						builder.append(entry.getValue());
						builder.append("\"");
					} else {
						builder.append(entry.getValue().toString());
					}
					builder.append(";");
				}

				builder.append(scriptCode);
				interp.eval(builder.toString());
			} catch (EvalError e) {
				e.printStackTrace();
			}

			// return result
			HashMap<String, Object> result = new HashMap<String, Object>();
			for (Entry<String, Object> entry : tuple.entrySet()) {
				Object value;
				try {
					value = interp.get(entry.getKey());
					result.put(entry.getKey(), value);
				} catch (EvalError e) {
					e.printStackTrace();
				}
			}
			return result;
		}
	};
}