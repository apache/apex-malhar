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

import com.google.common.collect.Maps;
import org.ektorp.ViewResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * <br>Couch-db input adaptor that emits a map.</br>
 * <br>This adaptor coverts the result of a database view to a map and emits it.</br>
 * <br>It uses the emitTuples implementation of {@link AbstractCouchDBInputOperator} which emits the complete result
 * of the ViewQuery every window cycle. </br>
 *
 * @since 0.3.5
 */
public abstract class AbstractMapBasedInputOperator extends AbstractCouchDBInputOperator<Map<Object, Object>>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractMapBasedInputOperator.class);

  @Override
  @SuppressWarnings("unchecked")
  public Map<Object, Object> getTuple(ViewResult.Row value)
  {
    Map<Object, Object> valueMap = Maps.newHashMap();
    try {
      valueMap = mapper.readValue(value.getValueAsNode(), valueMap.getClass());
    } catch (IOException e) {
      logger.error("Error converting to map : " + e.fillInStackTrace());
    }
    return valueMap;
  }
}
