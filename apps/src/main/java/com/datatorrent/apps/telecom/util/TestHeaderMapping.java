package com.datatorrent.apps.telecom.util;

import java.io.Serializable;

import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ift.CellProcessor;

import com.datatorrent.lib.parser.CSVHeaderMapping;

public class TestHeaderMapping implements CSVHeaderMapping, Serializable
{
  private String[] header;

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public CellProcessor[] getProcessors()
  {
      CellProcessor[] processor = new CellProcessor[header.length];
      for (CellProcessor cellProcessor : processor) {
        cellProcessor = new Optional();
      }
      return processor;
  }

  @Override
  public String[] getHeaders()
  {
    return header;
  }
  
  public void setHeaders(String[] headers){
    this.header = headers;
  }
}
