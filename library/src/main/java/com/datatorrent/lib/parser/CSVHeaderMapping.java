package com.datatorrent.lib.parser;

import org.supercsv.cellprocessor.ift.CellProcessor;

public interface CSVHeaderMapping
{
  
  CellProcessor[] getProcessors(); 
  
  String[] getHeaders();
  
}
