package com.datatorrent.contrib.lib.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoConstraint;
import org.anarres.lzo.LzoLibrary;
import org.anarres.lzo.LzopOutputStream;

import com.datatorrent.lib.io.fs.FilterStreamContext;
import com.datatorrent.lib.io.fs.FilterStreamProvider;

public class LzoStreamCodec
{

  public static class LZOFilterStreamContext extends FilterStreamContext.BaseFilterStreamContext<LZOpoutputStream>
  {
    public LZOFilterStreamContext(OutputStream outputStream) throws IOException
    {
      filterStream = new LZOpoutputStream(new LzopOutputStream(outputStream, LzoLibrary.getInstance().newCompressor(LzoAlgorithm.LZO1X, LzoConstraint.COMPRESSION)));
    }
  }

  static class LZOpoutputStream extends FilterOutputStream
  {
    public LZOpoutputStream(OutputStream outputStream)
    {
      super(outputStream);
    }
  }

  /**
   * A provider for LZO filter The file has to be rewritten from beginning when there is operator failure. The operates
   * on entries file and not on file parts.
   */
  public static class LZOFilterStreamProvider extends FilterStreamProvider.SimpleFilterReusableStreamProvider<LZOpoutputStream, OutputStream>
  {
    @Override
    protected FilterStreamContext<LZOpoutputStream> createFilterStreamContext(OutputStream outputStream) throws IOException
    {
      return new LZOFilterStreamContext(outputStream);
    }
  }
}
