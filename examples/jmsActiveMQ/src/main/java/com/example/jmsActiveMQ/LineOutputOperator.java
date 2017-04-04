package com.example.jmsActiveMQ;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.validation.constraints.NotNull;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**                                                                                                                                               
 * Converts each tuple to a string and writes it as a new line to the output file                                                                 
 */
public class LineOutputOperator extends AbstractFileOutputOperator<String>
{
  private static final String NL = System.lineSeparator();
  private static final Charset CS = StandardCharsets.UTF_8;

  @NotNull
  private String baseName;

  public String getBaseName() 
  { 
    return baseName;
  }

  public void setBaseName(String v) 
  { 
    baseName = v;
  }

  @Override
  protected String getFileName(String tuple)
  {
    return baseName;
  }

  @Override
  protected byte[] getBytesForTuple(String tuple)
  {
    String result = tuple + NL;
    return result.getBytes(CS);
  }
}
