/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.pigquery;


/**
 * This operator semantic is same as select top operator in stream query library. <br>
 * <p>
 * Operator must be used with percentage flag on. <br>
 * Please use operator : {@link com.datatorrent.lib.streamquery.SelectTopOperator}.
 * @displayName Pig Sample
 * @category Pig Query
 * @tags sample operator
 * @since 0.3.4
 */
@Deprecated
public class PigSampleOperator
{
  // Must not be used.
  private PigSampleOperator() 
  {
    assert(false);
  }
}
