package com.datatorrent.lib.io.fs;

public class AbstractReadAheadLineReaderTest extends AbstractBlockReaderTest
{
  @Override
  AbstractBlockReader.AbstractLineReader<String> getBlockReader()
  {
    return new ReadAheadBlockReader();
  }

  public static final class ReadAheadBlockReader extends AbstractBlockReader.AbstractReadAheadLineReader<String>
  {
    @Override
    protected String convertToRecord(byte[] bytes)
    {
      return new String(bytes);
    }
  }
}