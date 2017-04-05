package com.example.fileOutput;

import java.util.Arrays;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * Write incoming line to output file
 */
public class FileWriter extends AbstractFileOutputOperator<Long[]>
{
  private static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);
  private static final String CHARSET_NAME = "UTF-8";
  private static final String NL = System.lineSeparator();

  @NotNull
  private String fileName;           // current base file name

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

    LOG.debug("Leaving setup, fName = {}, id = {}", fName, id);
  }

  @Override
  protected String getFileName(Long[] tuple)
  {
    return fName;
  }

  @Override
  protected byte[] getBytesForTuple(Long[] pair)
  {
    byte result[] = null;
    try {
      result = (Arrays.toString(pair) + NL).getBytes(CHARSET_NAME);
    } catch (Exception e) {
      LOG.info("Error: got exception {}", e);
      throw new RuntimeException(e);
    }
    return result;
  }

  // getters and setters
  public String getFileName() { return fileName; }
  public void setFileName(String v) { fileName = v; }
}
