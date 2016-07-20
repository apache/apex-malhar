package com.example.myapexapp;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.validation.constraints.NotNull;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * Converts each tuple to a string and writes it as a new line to the output file
 */
public class LineOutputOperator extends AbstractFileOutputOperator<Object>
{
  private static final String NL = System.lineSeparator();
  private static final Charset CS = StandardCharsets.UTF_8;

  @NotNull
  private String baseName;

  @Override
  public byte[] getBytesForTuple(Object t) {
    String result = new String(t.toString().getBytes(), CS) + NL;
    return result.getBytes(CS);
 }

  @Override
  protected String getFileName(Object tuple) {
    return baseName;
  }

  public String getBaseName() { return baseName; }
  public void setBaseName(String v) { baseName = v; }
}
