/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.couchbase;

import com.datatorrent.common.util.DTThrowable;
import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * <p>CouchBaseJSONSerializer</p>
 *
 * @since 2.0.0
 */
public class CouchBaseJSONSerializer implements CouchBaseSerializer
{

  private ObjectMapper mapper;

  public CouchBaseJSONSerializer()
  {
    mapper = new ObjectMapper();
  }

  @Override
  public String serialize(Object o)
  {
    String value = null;
    try {
      value = mapper.writeValueAsString(o);
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    return value;
  }

}
