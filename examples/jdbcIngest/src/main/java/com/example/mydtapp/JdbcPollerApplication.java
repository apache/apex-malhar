package com.example.mydtapp;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.db.jdbc.JdbcPOJOPollInputOperator;
import com.datatorrent.lib.db.jdbc.JdbcStore;
import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.FieldInfo.SupportType;
import com.google.common.collect.Lists;

@ApplicationAnnotation(name = "PollJdbcToHDFSApp")
public class JdbcPollerApplication implements StreamingApplication
{
  public void populateDAG(DAG dag, Configuration conf)
  {
    JdbcPOJOPollInputOperator poller = dag.addOperator("JdbcPoller", new JdbcPOJOPollInputOperator());

    JdbcStore store = new JdbcStore();
    poller.setStore(store);

    poller.setFieldInfos(addFieldInfos());

    FileLineOutputOperator writer = dag.addOperator("Writer", new FileLineOutputOperator());
    dag.setInputPortAttribute(writer.input, PortContext.PARTITION_PARALLEL, true);
    writer.setRotationWindows(60);

    dag.addStream("dbrecords", poller.outputPort, writer.input);
  }

  /**
   * This method can be modified to have field mappings based on used defined
   * class
   */
  private List<FieldInfo> addFieldInfos()
  {
    List<FieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new FieldInfo("ACCOUNT_NO", "accountNumber", SupportType.INTEGER));
    fieldInfos.add(new FieldInfo("NAME", "name", SupportType.STRING));
    fieldInfos.add(new FieldInfo("AMOUNT", "amount", SupportType.INTEGER));
    return fieldInfos;
  }
}
