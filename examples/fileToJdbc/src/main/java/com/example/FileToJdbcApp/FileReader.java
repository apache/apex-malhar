package com.example.FileToJdbcApp;

import com.datatorrent.api.DefaultOutputPort;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;

public class FileReader extends LineByLineFileInputOperator{

  /**
   * output in bytes to match CsvParser input type
   */
  public final transient DefaultOutputPort<byte[]> byteOutput  = new DefaultOutputPort<>();

  @Override
  protected void emit(String tuple)
  {
    output.emit(tuple);
    byteOutput.emit(tuple.getBytes());
  }
}

