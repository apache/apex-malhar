/*
 * Copyright (c) 2015 DataTorrent, Inc.
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
package com.datatorrent.lib.appdata.snapshot;

import java.util.Map;

import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;

import com.datatorrent.lib.appdata.gpo.GPOGetters;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;

/**
 * This operator accepts a list of POJOs, and serves the data under the {@link SnapshotSchema}.
 * Each POJO represents a row in the table, and the full list of POJOs represents a table.
 * @displayName App Data Snapshot POJO Server
 * @category App Data
 * @tags appdata, snapshot, pojo
 */
public class AppDataSnapshotServerPOJO extends AbstractAppDataSnapshotServer<Object>
{
  /**
   * Flag indicating whether or not the first tuple was processed.
   */
  private boolean firstTupleProcessed = false;

  @NotNull
  private Map<String, String> fieldToGetter;
  /**
   * The getters for retrieving values from input POJOs.
   */
  private GPOGetters getters;

  /**
   * Create the operator.
   */
  public AppDataSnapshotServerPOJO()
  {
    //Do nothing
  }

  @Override
  public GPOMutable convert(Object inputEvent)
  {
    firstTuple(inputEvent);

    GPOMutable convertedResult = new GPOMutable(schema.getValuesDescriptor());

    GPOUtils.copyPOJOToGPO(convertedResult, getters, inputEvent);
    return convertedResult;
  }

  /**
   * A helper method which builds the getter methods for retrieving fields from pojos.
   * @param inputEvent An input POJO.
   */
  private void firstTuple(Object inputEvent)
  {
    if(firstTupleProcessed) {
      return;
    }

    firstTupleProcessed = true;

    Class<?> clazz = inputEvent.getClass();

    getters = GPOUtils.buildGPOGetters(fieldToGetter,
                                       schema.getValuesDescriptor(),
                                       clazz);
  }

  /**
   * This sets the fieldToGetter map. The fieldToGetter map defines how to retrieve a field specified
   * in the schema from an input POJO.
   * @param fieldToGetter The map from a field in the schema to its corresponding getter.
   */
  public void setFieldToGetter(@NotNull Map<String, String> fieldToGetter)
  {
    this.fieldToGetter = Preconditions.checkNotNull(fieldToGetter);
  }

  /**
   * This gets the fieldToGetter map.
   * @return The fieldToGetter map.
   */
  public Map<String, String> getFieldToGetter()
  {
    return fieldToGetter;
  }
}
