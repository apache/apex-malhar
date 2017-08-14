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
package org.apache.apex.malhar.contrib.geode;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;

/**
 * Function to create region dynamically through client API
 *
 *
 * @since 3.4.0
 */
public class RegionCreateFunction extends FunctionAdapter implements Declarable
{

  @Override
  public void init(Properties arg0)
  {
  }

  /**
   * Create a region in Geode cluster
   */
  @Override
  public void execute(FunctionContext context)
  {
    List<Object> args = (List<Object>)context.getArguments();
    String regionName = (String)args.get(0);

    try {
      Cache cache = CacheFactory.getAnyInstance();
      if (cache.getRegion(regionName) == null) {

        cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);
      }
    } catch (RegionExistsException re) {
      context.getResultSender().lastResult(new ArrayList<Integer>());
    } catch (CacheClosedException e) {
      context.getResultSender().lastResult(new ArrayList<Integer>());
    }
    context.getResultSender().lastResult(new ArrayList<Integer>());
  }

  /**
   * Return name to create Region
   */
  @Override
  public String getId()
  {
    return this.getClass().getName();
  }

  private static final long serialVersionUID = 2450085868879041729L;

}
