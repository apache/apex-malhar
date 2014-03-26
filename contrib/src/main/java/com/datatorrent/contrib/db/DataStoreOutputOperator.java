/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.db;

import java.util.ArrayList;
import java.util.List;

import com.datatorrent.lib.datamodel.converter.Converter;
import com.datatorrent.lib.db.AbstractStoreOutputOperator;
import com.datatorrent.lib.db.DataStoreWriter;


/**
 * Output operator to write tuples to given data store
 * Tuples are written in batches at each endwindow
 *
 * @param <INPUT> input type
 * @param <OUTPUT> output type
 */
public class DataStoreOutputOperator<INPUT, OUTPUT> extends AbstractStoreOutputOperator<INPUT, DataStoreWriter<OUTPUT>>
{
  /*
   * cache tuples to insert in end window
   */
  private List<OUTPUT> cache = new ArrayList<OUTPUT>();
  private long currentWindowId;
  /*
   * converter used to convert input type to output type
   */
  private Converter<INPUT, OUTPUT> converter;

  /**
   * converts input tuple type to output tuple type and caches them
   *
   * @param t input tuple
   */
  @Override
  public void processTuple(INPUT t)
  {
    cache.add(converter.convert(t));
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    super.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    // write to db
    store.batchInsert(cache, currentWindowId);
    cache.clear();
  }

  /**
   * Supply the converter to convert the input type to output type
   *
   * @param converter type converter
   */
  public void setConverter(Converter<INPUT, OUTPUT> converter)
  {
    this.converter = converter;
  }

}
