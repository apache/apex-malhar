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

import javax.validation.constraints.NotNull;

import com.datatorrent.lib.datamodel.converter.Converter;
import com.datatorrent.lib.db.*;

/**
 * Transactionable output operator to write tuples using the supplied data store writer.
 * Writes will be committed at the end window
 *
 * @param <INPUT> input tuple type
 * @param <OUTPUT> type expected by data store writer
 */
public class TransactionableDataStoreOutputOperator<INPUT, OUTPUT> extends AbstractTransactionableStoreOutputOperator<INPUT, TransactionableDataStoreWriter<OUTPUT>>
{
  /*
   * converter used to convert input type to output type
   */
  @NotNull
  private Converter<INPUT, OUTPUT> converter;

  /**
   * process tuple
   *
   * @param t input tuple
   */
  @Override
  public void processTuple(INPUT t)
  {
    OUTPUT outTuple = converter.convert(t);
    store.process(outTuple);
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

  @Override
  public void beginWindow(long windowId)
  {
    store.beginTransaction();
    super.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    store.commitTransaction();
    committedWindowId = currentWindowId;
  }

}
