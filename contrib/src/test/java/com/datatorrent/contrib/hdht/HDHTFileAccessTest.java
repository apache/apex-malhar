/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hdht;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.io.file.tfile.TFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import com.datatorrent.netlet.util.Slice;
import com.datatorrent.contrib.hdht.HDHTFileAccessFSImpl;
import com.datatorrent.contrib.hdht.HDHTWriter;
import com.datatorrent.contrib.hdht.HDHTFileAccess.HDSFileReader;
import com.datatorrent.contrib.hdht.HDHTFileAccess.HDSFileWriter;
import com.datatorrent.contrib.hdht.hfile.HFileImpl;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;

/**
 * Unit Test for HFile/TFile Writer/Reader With/Without compression.
 *
 * Use Writer, Reader to verify each other.
 *
 * To test reader, there is test cases for both sequential read and random read(existing/non-existing key)
 *
 */
public class HDHTFileAccessTest
{
  private byte[][] keys;

  private String[] values;

  private final String testFileDir = "target/hdsio";

  @Before
  public void setupData()
  {
    keys = new byte[][] { { 0 }, { 1 }, { 3 } };
    values = new String[] { "test0", "test1", "test3" };
  }


  @Test
  public void testTFile() throws IOException
  {
    testTFile(TFile.COMPRESSION_NONE);
  }

  @Test
  public void testTFileGZ() throws IOException
  {
    testTFile(TFile.COMPRESSION_GZ);
  }

  @Test
  public void testDTFile() throws IOException
  {
    testDTFile(TFile.COMPRESSION_NONE);
  }

  @Test
  public void testDTFileGZ() throws IOException
  {
    testDTFile(TFile.COMPRESSION_GZ);
  }

  @Test
  public void testHFile() throws IOException
  {
    testHFile(Algorithm.NONE);
  }

  @Test
  public void testHFileGZ() throws IOException
  {
    testHFile(Algorithm.GZ);
  }

  private void testTFile(String compression) throws IOException{

    TFileImpl timpl = new TFileImpl.DefaultTFileImpl();
    timpl.setCompressName(compression);
    writeFile(0, timpl, "TFileUnit" + compression);
    testSeqRead(0, timpl, "TFileUnit" + compression);
    testRandomRead(0, timpl, "TFileUnit" + compression);

  }

  private void testDTFile(String compression) throws IOException{

    TFileImpl timpl = new TFileImpl.DTFileImpl();
    timpl.setCompressName(compression);
    writeFile(0, timpl, "TFileUnit" + compression);
    testSeqRead(0, timpl, "TFileUnit" + compression);
    testRandomRead(0, timpl, "TFileUnit" + compression);

  }

  private void testHFile(Algorithm calgo) throws IOException{
    HFileImpl himpl = new HFileImpl();
    himpl.getConfigProperties().setProperty("hfile.block.cache.size", "0.5");
    himpl.setComparator(new HDHTWriter.DefaultKeyComparator());
    HFileContext context = new HFileContext();
    context.setCompression(calgo);
    himpl.setContext(context);
    writeFile(0, himpl, "HFileUnit" + calgo);
    testSeqRead(0, himpl, "HFileUnit" + calgo);
    testRandomRead(0, himpl, "HFileUnit" + calgo);
  }

  private void writeFile(long bucketKey, HDHTFileAccessFSImpl hfa, String fileName) throws IOException
  {
    File file = new File(testFileDir);
    FileUtils.deleteDirectory(file);
    hfa.setBasePath(testFileDir);
    hfa.init();
    //write to file
    HDSFileWriter out = hfa.getWriter(bucketKey, fileName);
    for (int i = 0; i < keys.length; i++) {
      out.append(keys[i], values[i].getBytes());
    }
    out.close();
  }

  private void testSeqRead(long bucketKey, HDHTFileAccessFSImpl hfa, String fileName) throws IOException
  {
    HDSFileReader in = hfa.getReader(bucketKey, fileName);
    Slice tkey = new Slice(null, 0, 0);
    Slice tvalue = new Slice(null, 0, 0);
    for (int i = 0; i < keys.length; i++) {
      assertTrue("If the cursor is currently not at the end. Next() method should return true ", in.next(tkey, tvalue));
      assertArrayEquals("Key is not as expected", keys[i], Arrays.copyOfRange(tkey.buffer, tkey.offset, tkey.offset + tkey.length));
      assertArrayEquals("Value is not as expected", values[i].getBytes(), Arrays.copyOfRange(tvalue.buffer, tvalue.offset, tvalue.offset + tvalue.length));
    }
    assertFalse("Should return false because cursor is at the end", in.next(tkey, tvalue));
    in.close();
  }

  private void testRandomRead(long bucketKey, HDHTFileAccessFSImpl hfa, String fileName) throws IOException
  {
    HDSFileReader in = hfa.getReader(bucketKey, fileName);
    Slice tkey = new Slice(null, 0, 0);
    Slice tval = new Slice(null, 0, 0);

    // seek to existing key k
    // seek() method should move to key[i] where key[i] = k
    // so next "next()" would return key[i] which is equal to k
    in.seek(new Slice(new byte[] { 1 }));
    assertTrue(in.next(tkey, tval));
    assertEquals("Value is not as expected", values[1], new String(Arrays.copyOfRange(tval.buffer, tval.offset, tval.offset + tval.length)));

    // If seek to non-existing key k
    // seek() method should move to key[i] where key[i-1] < k and key[i] > k
    // so next "next()"  would return first key[i] which is greater than k
    assertFalse(in.seek(new Slice(new byte[] { 2 })));
    in.next(tkey, tval);
    assertEquals("Value is not as expected", values[2], new String(Arrays.copyOfRange(tval.buffer, tval.offset, tval.offset + tval.length)));
    in.close();
  }

  @After
  public void cleanup() throws IOException
  {
    File file = new File(testFileDir);
    FileUtils.deleteDirectory(file);
  }

}
