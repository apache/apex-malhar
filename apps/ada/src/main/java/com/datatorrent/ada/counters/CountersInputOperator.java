/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.counters;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.appdata.qr.DataDeserializerFactory;
import com.datatorrent.lib.io.WebSocketServerInputOperator;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * TODO This is not fault tolerant.
 */
public class CountersInputOperator extends WebSocketServerInputOperator
{
  private transient LinkedBlockingQueue<CountersData> dataQueue = new LinkedBlockingQueue<CountersData>();

  public final transient DefaultOutputPort<CountersData> output = new DefaultOutputPort<CountersData>();

  @Override
  public void processMessage(String json)
  {
    DataDeserializerFactory ddf = new DataDeserializerFactory(CountersSchema.class,
                                                              CountersUpdateDataLogical.class);
    CountersData countersData = (CountersData) ddf.deserialize(json);
    dataQueue.add(countersData);
  }

  @Override
  public void emitTuples()
  {
    while(!dataQueue.isEmpty()) {
      output.emit(dataQueue.poll());
    }
  }
}
