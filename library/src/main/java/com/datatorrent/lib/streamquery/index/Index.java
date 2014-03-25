/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.streamquery.index;

import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * Interface to define sql select statement indexes for column names/aggregate.
 *
 * @since 0.3.2
 */
public interface Index
{
  /**
   * Function can key/value hash map, does metric implemented by sub class.
   */
	public void filter(@NotNull Map<String,Object> row, @NotNull Map<String, Object> collect);  
}
