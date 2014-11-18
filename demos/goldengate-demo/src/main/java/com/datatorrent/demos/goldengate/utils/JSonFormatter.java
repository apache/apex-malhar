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
package com.datatorrent.demos.goldengate.utils;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import com.goldengate.atg.datasource.DsConfiguration;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsTransaction;
import com.goldengate.atg.datasource.format.DsFormatterAdapter;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.goldengate.atg.datasource.meta.TableMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Example of serialize the goldengate transaction operation object<br>
 * The default goldengate objects(DsTransaction, DsOperation, etc) are not serializable in most ways.<br>
 * This example use the equivalent {@link _DsTransaction} objects that can be (de)serialized using any popular framework(In this case json format)
 */
public class JSonFormatter extends DsFormatterAdapter
{
  private static final Logger log = LoggerFactory.getLogger(JSonFormatter.class);

  @Override
  public void init(DsConfiguration conf, DsMetaData meta)
  {
    super.init(conf, meta);
    setSourceDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
    setTargetDateFormat("yyyy-MM-dd HH:mm:ss");
  }

  private ObjectMapper mapper = new ObjectMapper();
  {
    mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS"));
    mapper.getJsonFactory().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
  }

  @Override
  public void formatOp(DsTransaction tran, DsOperation op, TableMetaData tmd, PrintWriter pw)
  {
  }

  @Override
  public void endTx(DsTransaction tx, DsMetaData dbMeta, PrintWriter output)
  {
    super.endTx(tx, dbMeta, output);
  }

  @Override
  public void formatTx(DsTransaction tran, DsMetaData dma, PrintWriter pw)
  {
    try {
      log.info("Tran time" + tran.getStartTxTimeAsString() + "###" + tran.getReadTimeAsString());
      _DsTransaction _dt = new _DsTransaction();
      _dt.readFromDsTransaction(tran);
      mapper.writeValue(pw, _dt);
    } catch (Exception e) {
      log.error("Error serialize object", e);
    }
  }
}
