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

package com.datatorrent.demos.dimensions.ads.generic;

import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static com.datatorrent.demos.dimensions.ads.generic.AdsDimensionsDemo.*;



public class OperatorSettingTest {

  @Test
  public void testStore()
  {
    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();
    //Set input properties
    String eventSchema = SchemaUtils.jarResourceFileToString(EVENT_SCHEMA);

    store.setEventSchemaJSON(eventSchema);

    //Set store properties
    String dimensionalSchema = SchemaUtils.jarResourceFileToString(DIMENSIONAL_SCHEMA);
    String basePath = Preconditions.checkNotNull("Store");
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();
    basePath += Path.SEPARATOR + System.currentTimeMillis();
    hdsFile.setBasePath(basePath);
    System.out.println("Setting basePath " + basePath);
    store.setFileStore(hdsFile);
    store.getAppDataFormatter().setContinuousFormatString("#.00");
    store.setEventSchemaJSON(eventSchema);
    store.setDimensionalSchemaJSON(dimensionalSchema);
  }
}
