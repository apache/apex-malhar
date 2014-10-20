package com.datatorrent.contrib.goldengate.lib;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.goldengate.atg.datasource.AbstractHandler;
import com.goldengate.atg.datasource.DsConfiguration;
import com.goldengate.atg.datasource.DsEvent;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsTransaction;
import com.goldengate.atg.datasource.GGDataSource.Status;
import com.goldengate.atg.datasource.format.DsFormatter;
import com.goldengate.atg.datasource.meta.DsMetaData;



/**
 * An example of java plugin for goldengate. <br>
 * It capture each transaction and send it to kafka <br>
 * The format of kafka message depends on the DsFormatter defined in the property file <br><br>
 * Example of property for this handler:
 * <pre>
 *  <code>
 *  gg.handlerlist=kafkahandler
 *  gg.classpath=${yourpath}/jackson-core-asl-1.9.2.jar,${yourpath}/jackson-mapper-asl-1.9.2.jar,${yourpath}/kafka_2.8.0-0.8.0.jar,${yourpath}/rainerd-demo1-1.0-SNAPSHOT.jar
 *  gg.handler.kafkahandler.type=com.datatorrent.rainier.demo1.ogg.KafkaHandler
 *  gg.handler.kafkahandler.dateFormat=yyyy-MM-dd HH:mm:ss.SSSSSS
 *  # work only for transaction mode
 *  gg.handler.kafkahandler.mode=tx  
 *  # send message in sql format gg.handler.kafkahandler.format=com.goldengate.atg.datasource.format.SqlFormatter
 *  gg.handler.kafkahandler.format=com.datatorrent.rainier.demo1.ogg.JSonFormatter  # send message in json format
 *  gg.handler.kafkahandler.broker=broker1:9092,broker2:9092
 *  gg.handler.kafkahandler.producertype=async
 *  gg.handler.kafkahandler.topic=oggtopic
 *  </code>
 *</pre>
 *
 */
public class KafkaHandler extends AbstractHandler
{
  
  
  private static final Logger log = Logger.getLogger(KafkaHandler.class);

  static {
    log.setLevel(Level.ALL);
  }

  private Producer<String, String> producer;

  private String topic = "oggmsg";

  private String broker;

  private String partitioner;

  private String producertype = "async";

  private String serializer = "kafka.serializer.StringEncoder";

  private String keySerializer = "kafka.serializer.StringEncoder";

  private DsFormatter format = null;

  private PrintWriter pw;

  private StringBuffer sb;

  public void setPartitioner(String partitioner)
  {
    this.partitioner = partitioner;
  }

  public void setBroker(String broker)
  {
    this.broker = broker;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  public void setProducertype(String producertype)
  {
    this.producertype = producertype;
  }

  public void setSerializer(String serializer)
  {
    this.serializer = serializer;
  }

  public void setKeySerializer(String keySerializer)
  {
    this.keySerializer = keySerializer;
  }
  
  public DsFormatter getFormat()
  {
    return format;
  }
  
  public void setFormat(String format) throws Exception
  {
    this.format = (DsFormatter) Class.forName(format).newInstance();
  }

  @Override
  public void init(DsConfiguration config, DsMetaData meta)
  {
    super.init(config, meta);
    log.info("The default date format " + getDateFormat());
    config.setDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
    format.init(config, meta);
    format.setMode(getMode());
    log.info("Initializing kafka...");
    try {
      Properties props = new Properties();
      props.setProperty("serializer.class", serializer);
      props.setProperty("key.serializer.class", keySerializer);
      props.put("metadata.broker.list", broker);
      if (partitioner != null) {
        props.setProperty("partitioner.class", partitioner);
      }
      props.setProperty("producer.type", producertype);
      props.setProperty("topic.metadata.refresh.interval.ms", "100000");

      producer = new Producer<String, String>(new ProducerConfig(props));

      StringWriter sw = new StringWriter();
      sb = sw.getBuffer();
      pw = new PrintWriter(sw);

    } catch (Exception e) {
      log.error("Error initializing kafka...", e);
    }
  }

  @Override
  public Status operationAdded(DsEvent de, DsTransaction dst, DsOperation dso)
  {
    log.debug("Add operation" + de + ",   " + dso);
//    getFormat().formatOp(dst, dso, de.getMetaData().getTableMetaData(dso.getTableName()), pw);
    return super.operationAdded(de, dst, dso);
  }

  @Override
  public Status transactionRollback(DsEvent e, DsTransaction tx)
  {
    log.debug("Rollback the transaction " + e + ",   " + tx);
    return super.transactionRollback(e, tx);
  }
  
  @Override
  public Status transactionCommit(DsEvent de, DsTransaction tran)
  {
    log.debug("Commit the transaction " + de + ",   " + tran + ", at time " + tran.getTimestampAsString());

    for (DsOperation dsOperation : tran) {
      log.debug("Format the operation " + dsOperation);
      getFormat().formatOp(tran, dsOperation, de.getMetaData().getTableMetaData(dsOperation.getTableName()), pw);
    }
    
    log.debug("End the transaction ");
    getFormat().endTx(tran, de.getMetaData(), pw);
    
    log.debug("Format the transaction with " + getFormat());
    getFormat().formatTx(tran, de.getMetaData(), pw);
    log.debug("sending msg: " + sb.toString());
    producer.send(new KeyedMessage<String, String>(topic, getFormat().toString(), sb.toString()));
    log.debug("Send out the transaction " + de + ",   " + tran);
    sb.delete(0, sb.length());
    return super.transactionCommit(de, tran);
  }
  
  @Override
  public Status transactionBegin(DsEvent e, DsTransaction tx)
  {

    log.debug("Begin the transaction " + e + ",   " + tx);
    getFormat().beginTx(tx, e.getMetaData(), pw);
    return super.transactionBegin(e, tx);
  }

  @Override
  public String reportStatus()
  {
    return "Not implemented yet";
  }


}
