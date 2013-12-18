package com.datatorrent.apps.telecom.util;

import java.io.Serializable;

import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ift.CellProcessor;

import com.datatorrent.lib.parser.CSVHeaderMapping;

public class TestHeaderMapping implements CSVHeaderMapping, Serializable
{
  public String[] header;

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public CellProcessor[] getProcessors()
  {
    return new CellProcessor[] { new Optional(), new ParseBool(), new ParseDate("yyyy-MM-dd") };
  }

  @Override
  public String[] getHeaders()
  {
    return header;
  }
}
