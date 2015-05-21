/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.appdata.query;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context.OperatorContext;

/**
 * Not an operator
 * @author tfarkas
 * @param <QUERY_TYPE>
 * @param <META_QUERY>
 * @param <QUEUE_CONTEXT>
 */
public interface QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> extends Component<OperatorContext>
{
  public boolean enqueue(QUERY_TYPE query, META_QUERY metaQuery, QUEUE_CONTEXT queueContext);
  public QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> dequeue();
  public void beginWindow(long windowId);
  public void endWindow();
}
