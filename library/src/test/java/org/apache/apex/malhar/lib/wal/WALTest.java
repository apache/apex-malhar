/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.wal;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.Sets;

import com.datatorrent.lib.fileaccess.FileAccessFSImpl;

public class WALTest
{
  static final Random rand = new Random();
  private static final Logger logger = LoggerFactory.getLogger(WALTest.class);
  File file = new File("target/hds");

  static byte[] genRandomByteArray(int len)
  {
    byte[] val = new byte[len];
    rand.nextBytes(val);
    return val;
  }

  /**
   * - Write some data to WAL
   * - Read the data back. The amount of data read should be
   *   same as amount of data written.
   * @throws IOException
   */
  @Test
  public void testWalWriteAndRead() throws IOException
  {
    FileUtils.deleteDirectory(file);
    FileAccessFSImpl bfs = new MockFileAccess();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();

    int numTuples = 100;

    FSWal<byte[]> fsWal = new FSWal<>(bfs, new ByteArraySerde(), 1, file.getAbsoluteFile().toString() + "/1/WAL-0");

    WAL.WALWriter wWriter = fsWal.getWriter();
    for (int i = 0; i < numTuples; i++) {
      int len = rand.nextInt(100);
      wWriter.append(genRandomByteArray(len));
    }
    wWriter.close();

    File wal0 = new File(file.getAbsoluteFile().toString() + "/1/WAL-0");
    Assert.assertEquals("WAL file created ", true, wal0.exists());

    WAL.WALReader wReader = fsWal.getReader();
    int tuples = 0;
    while (wReader.advance()) {
      byte[] data = (byte[])wReader.get();
      tuples++;
    }
    wReader.close();
    Assert.assertEquals("Write and read same number of tuples ", numTuples, tuples);
  }

  /**
   * Read WAL from middle of the file by seeking to known valid
   * offset and start reading from that point till the end.
   */
  @Test
  public void testWalSkip() throws IOException
  {
    FileUtils.deleteDirectory(file);
    FileAccessFSImpl bfs = new MockFileAccess();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();

    long offset = 0;
    FSWal<byte[]> wal = new FSWal<>(bfs, new ByteArraySerde(), 1, file.getAbsoluteFile().toString() + "WAL-0");
    WAL.WALWriter wWriter = wal.getWriter();
    int totalTuples = 100;
    int recoveryTuples = 30;
    for (int i = 0; i < totalTuples; i++) {
      wWriter.append(genRandomByteArray(100));
      if (i == recoveryTuples) {
        offset = (long)wWriter.getOffset();
      }
    }
    logger.info("total file size is " + wWriter.getOffset() + " recovery offset is " + offset);
    wWriter.close();

    WAL.WALReader wReader = wal.getReader();
    wReader.seek(offset);
    int read = 0;
    while (wReader.advance()) {
      read++;
      wReader.get();
    }
    wReader.close();

    Assert.assertEquals("Number of tuples read after skipping", read, (totalTuples - recoveryTuples - 1));
  }

  /**
   * Test WAL rolling functionality, set segment size to 32 * 1024.
   * Write some data which will go over WAL segment size. multiple files
   * should be created.
   * @throws IOException
   */
  @Test
  public void testWalRolling() throws IOException
  {
    File file = new File("target/hds");
    FileUtils.deleteDirectory(file);

    FileAccessFSImpl bfs = new MockFileAccess();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();

    RollingFSWal<byte[]> wal = new RollingFSWal<>(bfs, new ByteArraySerde(), 1);
    wal.setBaseDir(file.getAbsoluteFile().toString());
    RollingFSWal<byte[]>.RollingFSWALWriter writer = (RollingFSWal<byte[]>.RollingFSWALWriter)wal.getWriter();
    writer.setMaxSegmentSize(32 * 1024);

    /* write each record of size 1K, each file will have 32 records, last file will have
     * (count % 32) records. */
    int wrote = 100;
    for (int i = 0; i < wrote; i++) {
      writer.append(genRandomByteArray(1020));
    }
    writer.close();

    /** Read from start till the end */
    RollingFSWal<byte[]>.RollingFSWalReader reader = (RollingFSWal<byte[]>.RollingFSWalReader)wal.getReader();
    reader.seek(new RollingFSWal.WalPointer(0, 0));

    int read = 0;
    while (reader.advance()) {
      Object o = reader.get();
      read++;
    }

    Assert.assertEquals("Number of records read from start ", wrote, read);

    /* skip first file for reading, and then read other entries till the end */
    reader.seek(new RollingFSWal.WalPointer(1, 0));

    read = 0;
    while (reader.advance()) {
      Object o = reader.get();
      read++;
    }
    Assert.assertEquals("Number of record read after skipping one file ", wrote - 32, read);

    /* skip first file and few records from the next file, and read till the end */
    reader.seek(new RollingFSWal.WalPointer(1, 16 * 1024));
    read = 0;
    while (reader.advance()) {
      Object o = reader.get();
      read++;
    }
    Assert.assertEquals("Number of record read after skipping one file ", wrote - (32 + 16), read);

  }

  static class ByteArraySerde implements WAL.Serde<byte[]>
  {
    @Override
    public byte[] toBytes(byte[] tuple)
    {
      return tuple;
    }

    @Override
    public byte[] fromBytes(byte[] data)
    {
      return data;
    }
  }

  /**
   * File storage for testing.
   */
  public static class MockFileAccess extends FileAccessFSImpl
  {
    private final Set<String> deletedFiles = Sets.newHashSet();

    public void disableChecksum()
    {
      fs.setVerifyChecksum(false);
    }

    @Override
    public void delete(long bucketKey, String fileName) throws IOException
    {
      super.delete(bucketKey, fileName);
      deletedFiles.add("" + bucketKey + fileName);
    }

    @Override
    public FileReader getReader(final long bucketKey, final String fileName) throws IOException
    {
      return null;
    }

    @Override
    public FileWriter getWriter(final long bucketKey, final String fileName) throws IOException
    {
      return null;
    }

  }

}
