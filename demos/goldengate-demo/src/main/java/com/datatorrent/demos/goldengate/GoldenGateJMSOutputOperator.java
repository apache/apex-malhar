/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.goldengate;

import com.datatorrent.demos.goldengate.utils._DsTransaction;
import com.datatorrent.demos.goldengate.utils._DsOperation;
import com.datatorrent.demos.goldengate.utils._DsColumn;
import com.datatorrent.lib.io.jms.AbstractActiveMQSinglePortOutputOperator;
import com.goldengate.atg.datasource.DsOperation.OpType;
import java.text.SimpleDateFormat;
import javax.jms.JMSException;
import javax.jms.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This operator sends JMS messages to Golden Gate in the XML format expected by our Golden Gate
 * configuration.
 */
public class GoldenGateJMSOutputOperator extends AbstractActiveMQSinglePortOutputOperator<_DsTransaction>
{
  private static transient Logger logger = LoggerFactory.getLogger(GoldenGateJMSOutputOperator.class);
  private transient SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");

  @Override
  protected Message createMessage(_DsTransaction tuple)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("<t>\n");

    String date = dateFormat.format(tuple.getReadTime());

    for(_DsOperation op: tuple.getOps()) {
      if(op.getOperationType() != OpType.DO_INSERT) {
        continue;
      }

      sb.append("<o t='");
      sb.append(op.getTableName().getFullName());
      sb.append("' s='I' d='");
      sb.append(date);
      sb.append("' p='");
      sb.append(op.getPositionSeqno());
      sb.append("'>\n");

      _DsColumn[] cols = op.getCols().toArray(new _DsColumn[] {});

      for(int columnCounter = 0;
          columnCounter < 3;
          columnCounter++) {
        sb.append("<c i='");
        sb.append(columnCounter);
        sb.append("'>");
        sb.append("<a><![CDATA[");
        sb.append(cols[columnCounter].getAfterValue());
        sb.append("]]></a></c>\n");
      }

      sb.append("</o>\n");
    }

    sb.append("</t>\n");
    Message message = null;

    logger.info(sb.toString());

    try {
      message = getSession().createTextMessage(sb.toString());
    }
    catch (JMSException ex) {
      throw new RuntimeException(ex);
    }

    return message;
  }
}
