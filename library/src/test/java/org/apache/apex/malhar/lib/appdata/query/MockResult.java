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
package org.apache.apex.malhar.lib.appdata.query;

import org.apache.apex.malhar.lib.appdata.query.serde.MessageSerializerInfo;
import org.apache.apex.malhar.lib.appdata.query.serde.MessageType;
import org.apache.apex.malhar.lib.appdata.schemas.Query;
import org.apache.apex.malhar.lib.appdata.schemas.Result;

@MessageType(type = MockResult.TYPE)
@MessageSerializerInfo(clazz = MockResultSerializer.class)
public class MockResult extends Result
{
  public static final String TYPE = "dataResult";

  public MockResult(Query query)
  {
    super(query);
  }

  public MockResult(Query query, long countdown)
  {
    super(query, countdown);
  }
}
