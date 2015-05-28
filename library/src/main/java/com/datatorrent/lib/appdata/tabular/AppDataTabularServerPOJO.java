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

package com.datatorrent.lib.appdata.tabular;

import com.datatorrent.lib.appdata.gpo.GPOGetters;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.google.common.base.Preconditions;
import javax.validation.constraints.NotNull;

import java.util.Map;


public class AppDataTabularServerPOJO extends AbstractAppDataTabularServer<Object>
{
  private boolean firstTuple = true;

  @NotNull
  private Map<String, String> fieldToGetter;
  private GPOGetters getters;

  public AppDataTabularServerPOJO()
  {
  }

  @Override
  public GPOMutable convert(Object inputEvent)
  {
    firstTuple(inputEvent);

    GPOMutable convertedResult = new GPOMutable(schema.getValuesDescriptor());

    GPOUtils.copyPOJOToGPO(convertedResult, getters, inputEvent);
    return convertedResult;
  }

  private void firstTuple(Object inputEvent)
  {
    if(!firstTuple) {
      return;
    }

    Class<?> clazz = inputEvent.getClass();

    getters = GPOUtils.buildGPOGetters(fieldToGetter,
                                       schema.getValuesDescriptor(),
                                       clazz);
  }

  public void setFieldToGetter(@NotNull Map<String, String> fieldToGetter)
  {
    this.fieldToGetter = Preconditions.checkNotNull(fieldToGetter);
  }

  public Map<String, String> getFieldToGetter()
  {
    return fieldToGetter;
  }
}
