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
package org.apache.apex.malhar.python.operator.transform;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.PythonConstants;
import org.apache.apex.malhar.python.operator.PythonGenericOperator;

public class PythonFlatMapOperator<T> extends PythonGenericOperator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonFlatMapOperator.class);

  public PythonFlatMapOperator()
  {
    this(null);
  }

  public PythonFlatMapOperator(byte[] serializedFunc)
  {

    super(PythonConstants.OpType.FLAT_MAP, serializedFunc);
  }

  @Override
  protected void processTuple(T tuple)
  {
    LOG.trace("Received Tuple: {}" + tuple);
    List<T> result = (List<T>)getServer().getProxy().execute(tuple);
    if (result != null) {
      LOG.trace("List response received: {}" + result);
      if (result instanceof List) {
        for (T item : result) {
          out.emit(item);
        }
      } else {
        LOG.warn("Returned response is not list: {}" + result);
      }
    }
  }

}
