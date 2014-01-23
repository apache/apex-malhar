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

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Implementation of {@link AbstractCouchDBOutputOperator} that saves a Map in the couch database<br></br>
 *
 * @since 0.3.5
 */
public class MapBasedCouchDbOutputOperator extends AbstractCouchDBOutputOperator<Map<Object, Object>>
{

  private final EventForMap couchDbEvent;

  public MapBasedCouchDbOutputOperator()
  {
    super();
    couchDbEvent = new EventForMap();
  }

  @Override
  public CouchDbEvent getCouchDbEventFrom(Map<Object, Object> tuple)
  {
    couchDbEvent.setPayload(tuple);
    return couchDbEvent;
  }

  private class EventForMap implements CouchDbEvent
  {

    private Map<Object, Object> payload;

    void setPayload(@Nonnull Map<Object, Object> payload)
    {
      this.payload = payload;
    }

    @Nullable
    @Override
    public String getId()
    {
      return (String) payload.get("_id");
    }

    @Nonnull
    @Override
    public Map<Object, Object> getPayLoad()
    {
      return payload;
    }
  }
}
