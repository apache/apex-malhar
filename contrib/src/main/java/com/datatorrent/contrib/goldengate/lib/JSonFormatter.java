package com.datatorrent.contrib.goldengate.lib;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import com.goldengate.atg.datasource.DsConfiguration;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsTransaction;
import com.goldengate.atg.datasource.format.DsFormatterAdapter;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.goldengate.atg.datasource.meta.TableMetaData;

/**
 * An Example of serialize the goldengate transaction operation object<br>
 * The default goldengate objects(DsTransaction, DsOperation, etc) are not serializable in most ways.<br>
 * This example use the equivalent {@link _DsTransaction} objects that can be (de)serialized using any popular framework(In this case json format)
 *
 */
public class JSonFormatter extends DsFormatterAdapter
{

  private static final Logger log = Logger.getLogger(JSonFormatter.class);

  static {
    log.setLevel(Level.ALL);
  }
//
//  private static String seperator = "\n=====\n";
//
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
    // Comment out this for op mode
//    try {
//      log.info("Tran time" + tran.getStartTxTimeAsString() + "###" + tran.getReadTimeAsString());
//      _DsTransaction _dt = new _DsTransaction();
//      _dt.readFromDsTransaction(tran);
//      mapper.writeValue(pw, _dt);
//      pw.write(seperator);
//      mapper.writeValue(pw, op);
//      pw.write(seperator);
//      mapper.writeValue(pw, tmd);
//    } catch (Exception e) {
//      log.error("Error serialize object", e);
//    }
  }

  @Override
  public void endTx(DsTransaction tx, DsMetaData dbMeta, PrintWriter output)
  {
    super.endTx(tx, dbMeta, output);
  }



  @Override
  public void formatTx(DsTransaction tran, DsMetaData dma, PrintWriter pw)
  {
    // works for tx mode
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
