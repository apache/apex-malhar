package com.example.myapexapp;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.validation.constraints.NotNull;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * Converts each tuple to a string and writes it as a new line to the output file
 */
public class LineOutputOperator extends AbstractFileOutputOperator<byte[]>
{
  private static final String NL = System.lineSeparator();
  private static final Charset CS = StandardCharsets.UTF_8;

  @NotNull
  private String baseName;

  @Override
  public byte[] getBytesForTuple(byte[] t) {
    String result = new String(t, CS) + NL;
    return result.getBytes(CS);
 }

  @Override
  protected String getFileName(byte[] tuple) {
    return baseName;
  }

  public String getBaseName() { return baseName; }
  public void setBaseName(String v) { baseName = v; }
}
