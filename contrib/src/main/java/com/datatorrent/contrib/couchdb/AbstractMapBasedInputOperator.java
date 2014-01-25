/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.couchdb;

import com.datatorrent.api.annotation.ShipContainingJars;
import org.codehaus.jackson.map.ObjectMapper;
import org.ektorp.ViewResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Couch-db input adaptor that emits a map.<br/>
 * This adaptor coverts the result of a database view to a map and emits it.<br/>
 * It uses the emitTuples implementation of {@link AbstractCouchDBInputOperator} which emits the complete result
 * of the ViewQuery every window cycle.
 *
 * @since 0.3.5
 */
@ShipContainingJars(classes = {ObjectMapper.class})
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
