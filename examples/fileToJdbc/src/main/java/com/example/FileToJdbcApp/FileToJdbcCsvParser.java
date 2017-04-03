package com.example.FileToJdbcApp;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.db.jdbc.JdbcFieldInfo;
import com.datatorrent.lib.db.jdbc.JdbcPOJOInsertOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

@ApplicationAnnotation(name = "FileToJdbcCsvParser")
public class FileToJdbcCsvParser implements StreamingApplication{

  @Override
  public void populateDAG(DAG dag, Configuration configuration) {
    // create operators
    FileReader fileReader = dag.addOperator("FileReader", FileReader.class);
    CsvParser csvParser = dag.addOperator("CsvParser", CsvParser.class);
    JdbcPOJOInsertOutputOperator jdbcOutputOperator = dag.addOperator("JdbcOutput", JdbcPOJOInsertOutputOperator.class);

    // configure operators
    String pojoSchema = SchemaUtils.jarResourceFileToString("schema.json");
    csvParser.setSchema(pojoSchema);

    jdbcOutputOperator.setFieldInfos(addFieldInfos());
    JdbcTransactionalStore outputStore = new JdbcTransactionalStore();
    jdbcOutputOperator.setStore(outputStore);

    // add stream
    dag.addStream("Bytes", fileReader.byteOutput, csvParser.in);
    dag.addStream("POJOs", csvParser.out, jdbcOutputOperator.input);
  }

    /**
     * This method can be modified to have field mappings based on used defined
     * class
     */
  private List<JdbcFieldInfo> addFieldInfos() {
    List<JdbcFieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new JdbcFieldInfo("ACCOUNT_NO", "accountNumber", JdbcFieldInfo.SupportType.INTEGER , INTEGER));
    fieldInfos.add(new JdbcFieldInfo("NAME", "name", JdbcFieldInfo.SupportType.STRING, VARCHAR));
    fieldInfos.add(new JdbcFieldInfo("AMOUNT", "amount", JdbcFieldInfo.SupportType.INTEGER, INTEGER));
    return fieldInfos;
  }
}

