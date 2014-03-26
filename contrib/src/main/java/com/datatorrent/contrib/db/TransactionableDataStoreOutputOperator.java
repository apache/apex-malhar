/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import com.datatorrent.lib.datamodel.converter.Converter;
import com.datatorrent.lib.db.*;

import com.datatorrent.api.Context.OperatorContext;

/**
 *
 * @param <INPUT>
 * @param <OUTPUT>
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class TransactionableDataStoreOutputOperator<INPUT, OUTPUT> extends AbstractAggregateTransactionableStoreOutputOperator<INPUT, TransactionalDataStoreWriter<OUTPUT>>
{
  /*
   * cache tuples to insert in end window
   */
  private List<OUTPUT> cache = new ArrayList<OUTPUT>();
  /*
   * converter used to convert input type to output type
   */
  @NotNull
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
  public void storeAggregate()
  {
    // write to db
    store.batchInsert(cache, currentWindowId);
    cache.clear();
  }


}
