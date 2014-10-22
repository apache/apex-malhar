/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.goldengate.lib;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.goldengate.atg.datasource.DsOperation.OpType;
import java.io.IOException;
import java.text.SimpleDateFormat;
import kafka.message.Message;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaInput extends  KafkaSinglePortStringInputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaInput.class);
  public final transient DefaultOutputPort<Employee> employeePort = new DefaultOutputPort<Employee>();

  private transient ObjectMapper mapper = new ObjectMapper();

  @Override
  public void setup(OperatorContext context)
  {
    mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS"));
  }

  @Override
  public String getTuple(Message message)
  {
    String result = super.getTuple(message);
    logger.debug("recieved message {} " + result);
    return result;
  }

  @Override
  public void emitTuple(Message msg)
  {
    String tupleJSON = getTuple(msg);
    outputPort.emit(tupleJSON);

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

      employeePort.emit(employee);
    }
  }
}