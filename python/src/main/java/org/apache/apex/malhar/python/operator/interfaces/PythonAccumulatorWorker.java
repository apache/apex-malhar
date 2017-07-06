/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.python.operator.interfaces;

import java.util.Map;

public interface PythonAccumulatorWorker<T> extends PythonWorker<T>
{
  public T setObject(byte[] obj, String opType);

  public T defaultAccumulatedValue();

  public T accumulate(T accumulatedValue, T input);

  public T merge(T accumulatedValue1, T accumulatedValue2);

  public T getOutput(T accumulatedValue);

  public T getRetraction(T output);

}
