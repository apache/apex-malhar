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

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * Implementation of {@link AbstractCouchDBOutputOperator}  that saves a Map in the couch database<br></br>
 *
 * @since 0.3.5
 */
public class MapBasedCouchDbOutputOperator extends AbstractCouchDBOutputOperator<Map<Object, Object>>
{

  @Override
  public CouchDbUpdateCommand getCommandToUpdateDb(Map<Object, Object> tuple)
  {
    return new UpdateCommandForMap(tuple);
  }

  private class UpdateCommandForMap implements CouchDbUpdateCommand
  {

    private final Map<Object, Object> payload;

    public UpdateCommandForMap(Map<Object, Object> payload)
    {
      this.payload = Preconditions.checkNotNull(payload, "payload");
    }

    @Nullable
    @Override
    public String getId()
    {
      return (String) payload.get("_id");
    }

    @Nullable
    @Override
    public String getRevision()
    {
      return (String) payload.get("_rev");
    }

    @Nonnull
    @Override
    public Map<Object, Object> getPayLoad()
    {
      return payload;
    }

    @Override
    public void setRevision(String revision)
    {
      payload.put("_rev", revision);
    }

    @Override
    public void setId(String id)
    {
      payload.put("_id", id);
    }
  }
}
