/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.couchdb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.ektorp.ViewResult;

/**
 * A base CouchDb input adaptor that emits a map. <br/>
 * <p>
 * This adaptor coverts the result of a database view to a map and emits it.<br/>
 * It uses the emitTuples implementation of {@link AbstractCouchDBInputOperator} which emits the complete result
 * of the ViewQuery every window cycle.
 * @displayName Abstract Map Based Input
 * @category Input
 * @tags input operator
 * @since 0.3.5
 */
public abstract class AbstractMapBasedInputOperator extends AbstractCouchDBInputOperator<Map<Object, Object>>
{
  private transient ObjectMapper mapper;

  public AbstractMapBasedInputOperator()
  {
    mapper = new ObjectMapper();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<Object, Object> getTuple(ViewResult.Row value) throws IOException
  {
    return mapper.readValue(value.getValueAsNode(), HashMap.class);
  }
}
