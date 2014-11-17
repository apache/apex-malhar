/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.goldengate.lib;

import com.datatorrent.demos.goldengate.utils._DsTransaction;
import com.datatorrent.demos.goldengate.utils._DsOperation;
import com.datatorrent.demos.goldengate.utils._DsColumn;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;

import com.goldengate.atg.datasource.DsOperation.OpType;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.contrib.kafka.AbstractKafkaInputOperator;
import com.datatorrent.contrib.kafka.KafkaConsumer;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.common.util.DTThrowable;

import kafka.message.Message;

public class KafkaInput extends AbstractKafkaInputOperator<KafkaConsumer>
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaInput.class);

  public final transient DefaultOutputPort<Employee> outputPort = new DefaultOutputPort<Employee>();
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<_DsTransaction> transactionPort = new DefaultOutputPort<_DsTransaction>();

  private transient ObjectMapper mapper = new ObjectMapper();

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS"));
  }

  public String getMessageString(Message message)
  {
    String data = "";
    try {
      ByteBuffer buffer = message.payload();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      data = new String(bytes);
      //logger.debug("Consuming {}", data);
    }
    catch (Exception ex) {
      DTThrowable.rethrow(ex);
    }
    return data;
  }

  @Override
  public void emitTuple(Message msg)
  {
    String tupleJSON = getMessageString(msg);

    System.out.println(tupleJSON);
    // enrichment and save to mysql
    _DsTransaction _dt = null;

    try {
      _dt = mapper.readValue(tupleJSON, _DsTransaction.class);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    for(_DsOperation op: _dt.getOps()) {
      if(op.getOperationType() != OpType.DO_INSERT) {
        continue;
      }

      _DsColumn[] cols = op.getCols().toArray(new _DsColumn[] {});

      Employee employee = new Employee();
      employee.eid = Integer.parseInt(cols[0].getAfterValue());
      employee.ename = cols[1].getAfterValue();
      employee.did = Integer.parseInt(cols[2].getAfterValue());

      outputPort.emit(employee);
    }

    if (transactionPort.isConnected()) {
      transactionPort.emit(_dt);
    }
  }
}