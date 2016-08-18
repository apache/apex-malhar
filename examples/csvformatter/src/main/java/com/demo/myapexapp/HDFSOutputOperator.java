package com.demo.myapexapp;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * HDFSoutput operator with implementation to write Objects to HDFS
 *
 * @param <T>
 */
public class HDFSOutputOperator<T> extends AbstractFileOutputOperator<T>
{

  @NotNull
  String outFileName;

  //setting default value
  String lineDelimiter = "\n";

  //Switch to write the files to HDFS - set to false to diable writes 
  private boolean writeFilesFlag = true;

  int id;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    id = context.getId();
  }

  public boolean isWriteFilesFlag()
  {
    return writeFilesFlag;
  }

  public void setWriteFilesFlag(boolean writeFilesFlag)
  {
    this.writeFilesFlag = writeFilesFlag;
  }

  public String getOutFileName()
  {
    return outFileName;
  }

  public void setOutFileName(String outFileName)
  {
    this.outFileName = outFileName;
  }

  @Override
  protected String getFileName(T tuple)
  {
    return getOutFileName() + id;
  }

  public String getLineDelimiter()
  {
    return lineDelimiter;
  }

  public void setLineDelimiter(String lineDelimiter)
  {
    this.lineDelimiter = lineDelimiter;
  }

  @Override
  protected byte[] getBytesForTuple(T tuple)
  {
    String temp = tuple.toString().concat(String.valueOf(lineDelimiter));
    byte[] theByteArray = temp.getBytes();

    return theByteArray;
  }

  @Override
  protected void processTuple(T tuple)
  {
    if (writeFilesFlag) {
    }
    super.processTuple(tuple);
  }

}
