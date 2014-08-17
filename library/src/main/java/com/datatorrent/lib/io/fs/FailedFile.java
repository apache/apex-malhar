package com.datatorrent.lib.io.fs;

  /**
   * Class representing failed file, When read fails on a file in middle, then the file is
   * added to failedList along with last read offset.
   * The files from failedList will be processed after all pendingFiles are processed, but
   * before checking for new files.
   * failed file is retried for maxRetryCount number of times, after that the file is
   * ignored.
   */
  class FailedFile 
  {
    String path;
    int   offset;
    int    retryCount;
    long   lastFailedTime;

    /* For kryo serialization */
    protected FailedFile() {}

    protected FailedFile(String path, int offset) {
      this.path = path;
      this.offset = offset;
      this.retryCount = 0;
    }

    protected FailedFile(String path, int offset, int retryCount) {
      this.path = path;
      this.offset = offset;
      this.retryCount = retryCount;
    }

    @Override
    public String toString()
    {
      return "FailedFile[" +
          "path='" + path + '\'' +
          ", offset=" + offset +
          ", retryCount=" + retryCount +
          ", lastFailedTime=" + lastFailedTime +
          ']';
    }
  }
