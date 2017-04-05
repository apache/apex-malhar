package com.example.myapexapp;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.Context;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * Write incoming lines to output file
 */
public class FileOutputOperator extends AbstractFileOutputOperator<String>
{
  private static final String CHARSET_NAME = "UTF-8";
  private static final String NL = System.lineSeparator();

  @NotNull
  private String fileName;

  private transient String fName;    // per partition file name

  @Override
  public void setup(Context.OperatorContext context)
  {
    // create file name for this partition by appending the operator id to
    // the base name
    //
    long id = context.getId();
    fName = fileName + "_p" + id;
    super.setup(context);
  }

  @Override
  protected String getFileName(String tuple)
  {
    return fName;
  }

  @Override
  protected byte[] getBytesForTuple(String line)
  {
    byte result[] = null;
    try {
      result = (line + NL).getBytes(CHARSET_NAME);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  // getters and setters
  public String getFileName() { return fileName; }
  public void setFileName(String v) { fileName = v; }
}
