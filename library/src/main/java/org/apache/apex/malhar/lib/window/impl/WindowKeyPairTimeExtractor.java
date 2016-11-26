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
package org.apache.apex.malhar.lib.window.impl;

import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.commons.lang3.tuple.Pair;

/**
 * A {@link TimeExtractor} to extract time from Pair of {@link Window} and key
 * The type of key doesn't matter in this case, so it assumes object as the key type
 *
 * @since 3.6.0
 */
public class WindowKeyPairTimeExtractor<K> implements TimeExtractor<Pair<Window, K>>
{

  private final WindowTimeExtractor windowTimeExtractor = new WindowTimeExtractor();

  @Override
  public long getTime(Pair<Window, K> windowKeyPair)
  {
    return windowTimeExtractor.getTime(windowKeyPair.getKey());
  }

}
