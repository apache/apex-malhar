package com.example.FileToJdbcApp;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.db.jdbc.JdbcFieldInfo;
import com.datatorrent.lib.db.jdbc.JdbcPOJOInsertOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

@ApplicationAnnotation(name = "FileToJdbcCustomParser")
public class FileToJdbcCustomParser implements StreamingApplication{

  @Override
  public void populateDAG(DAG dag, Configuration configuration) {
    // create operators
    FileReader fileReader = dag.addOperator("FileReader", FileReader.class);
    CustomParser customParser = dag.addOperator("CustomParser", CustomParser.class);
    JdbcPOJOInsertOutputOperator jdbcOutputOperator = dag.addOperator("JdbcOutput", JdbcPOJOInsertOutputOperator.class);

    // configure operators
    jdbcOutputOperator.setFieldInfos(addFieldInfos());
    JdbcTransactionalStore outputStore = new JdbcTransactionalStore();
    jdbcOutputOperator.setStore(outputStore);

    // add stream
    dag.addStream("Data", fileReader.output, customParser.input);
    dag.addStream("POJOs", customParser.output, jdbcOutputOperator.input);
  }

  /**
   * This method can be modified to have field mappings based on used defined
   * class
   */
  private List<JdbcFieldInfo> addFieldInfos()
  {
    List<JdbcFieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new JdbcFieldInfo("ACCOUNT_NO", "accountNumber", JdbcFieldInfo.SupportType.INTEGER , INTEGER));
    fieldInfos.add(new JdbcFieldInfo("NAME", "name", JdbcFieldInfo.SupportType.STRING, VARCHAR));
    fieldInfos.add(new JdbcFieldInfo("AMOUNT", "amount", JdbcFieldInfo.SupportType.INTEGER, INTEGER));
    return fieldInfos;
  }
}

