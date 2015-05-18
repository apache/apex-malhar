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
package com.datatorrent.lib.pigquery.condition;

import java.util.Map;

import javax.validation.constraints.NotNull;


/**
 * Interface for pig group by condition.  
 * <p>
 * @displayName Pig Group Condition
 * @category Pig Query
 * @tags group, condition, map, string
 * @since 0.3.4
 */
@Deprecated
public interface PigGroupCondition
{
  public Object compute(@NotNull Map<String, Object> tuple);
}
