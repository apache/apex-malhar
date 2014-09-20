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
package com.datatorrent.lib.pigquery;

/**
 * Implements pig order by semantic on live steam.  <br>
 * <p>
 * This semantic same as sql stream query  order by  operator. <br>
 * Please use operator  {@link com.datatorrent.lib.streamquery.OrderByOperator}
 * @displayName: Pig OrderBy
 * @category: pigquery
 * @tag: orderby operator
 * @since 0.3.4
 */
public class PigOrderByOperator 
{
  // Must not be used.
  private PigOrderByOperator()
  {
    assert(false);
  }
}
