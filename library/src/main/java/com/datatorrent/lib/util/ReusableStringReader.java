package com.datatorrent.lib.util;

import java.io.IOException;
import java.io.Reader;

public class ReusableStringReader extends Reader
  {
    private String str;
    private int length;
    private int next = 0;

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException
    {
      ensureOpen();
      if ((off < 0) || (off > cbuf.length) || (len < 0) || ((off + len) > cbuf.length) || ((off + len) < 0)) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }
      if (next >= length)
        return -1;
      int n = Math.min(length - next, len);
      str.getChars(next, next + n, cbuf, off);
      next += n;
      return n;
    }

    /**
* Reads a single character.
*
* @return The character read, or -1 if the end of the stream has been reached
*
* @exception IOException
* If an I/O error occurs
*/
    public int read() throws IOException
    {
      ensureOpen();
      if (next >= length)
        return -1;
      return str.charAt(next++);
    }

    public boolean ready() throws IOException
    {
      ensureOpen();
      return true;
    }

    @Override
    public void close() throws IOException
    {
      str = null;
    }

    /** Check to make sure that the stream has not been closed */
    private void ensureOpen() throws IOException
    {
      if (str == null) {
        throw new IOException("Stream closed");
      }
    }

    public void open(String str) throws IOException
    {
      this.str = str;
      this.length = this.str.length();
      this.next = 0;
    }

  }