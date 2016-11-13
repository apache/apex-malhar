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
package org.apache.hadoop.io.file.tfile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;

public class DTFileTest
{
  private static String ROOT =
      System.getProperty("test.build.data", "target/tfile-test");

  private Configuration conf;
  private Path path;
  private FileSystem fs;
  private NanoTimer timer;
  private Random rng;
  private RandomDistribution.DiscreteRNG keyLenGen;
  private KVGenerator kvGen;


  static class TestConf
  {
    public int minWordLen = 5;
    public int maxWordLen = 20;
    public int dictSize = 1000;
    int minKeyLen = 10;
    int maxKeyLen = 50;
    int minValLength = 100;
    int maxValLength = 200;
    int minBlockSize = 64 * 1024;
    int fsOutputBufferSize = 1;
    int fsInputBufferSize = 256 * 1024;
    long fileSize = 3 * 1024 * 1024;
    long seekCount = 1000;
    String compress = "gz";

  }

  TestConf tconf = new TestConf();

  public void setUp() throws IOException
  {
    conf = new Configuration();

    conf.setInt("tfile.fs.input.buffer.size", tconf.fsInputBufferSize);
    conf.setInt("tfile.fs.output.buffer.size", tconf.fsOutputBufferSize);
    path = new Path(ROOT, "dtfile");
    fs = path.getFileSystem(conf);
    timer = new NanoTimer(false);
    rng = new Random();
    keyLenGen = new RandomDistribution.Zipf(new Random(rng.nextLong()), tconf.minKeyLen, tconf.maxKeyLen, 1.2);
    RandomDistribution.DiscreteRNG valLenGen = new RandomDistribution.Flat(new Random(rng.nextLong()),
        tconf.minValLength, tconf.maxValLength);
    RandomDistribution.DiscreteRNG wordLenGen = new RandomDistribution.Flat(new Random(rng.nextLong()),
        tconf.minWordLen, tconf.maxWordLen);
    kvGen = new KVGenerator(rng, true, keyLenGen, valLenGen, wordLenGen,
        tconf.dictSize);
  }


  private static FSDataOutputStream createFSOutput(Path name, FileSystem fs) throws IOException
  {
    if (fs.exists(name)) {
      fs.delete(name, true);
    }
    FSDataOutputStream fout = fs.create(name);
    return fout;
  }

  int tuples = 0;

  private void writeTFile() throws IOException
  {

    FSDataOutputStream fout = createFSOutput(path, fs);
    byte[] key = new byte[16];
    ByteBuffer bb = ByteBuffer.wrap(key);
    try {
      DTFile.Writer writer = new DTFile.Writer(fout, tconf.minBlockSize, tconf.compress, "memcmp", conf);
      try {
        BytesWritable tmpKey = new BytesWritable();
        BytesWritable val = new BytesWritable();
        for (long i = 0; true; ++i) {
          if (i % 1000 == 0) { // test the size for every 1000 rows.
            if (fs.getFileStatus(path).getLen() >= tconf.fileSize) {
              break;
            }
          }
          bb.clear();
          bb.putLong(i);
          kvGen.next(tmpKey, val, false);
          writer.append(key, 0, key.length, val.get(), 0, val
              .getSize());
          tuples++;
        }
      } finally {
        writer.close();
      }
    } finally {
      fout.close();
    }

    long fsize = fs.getFileStatus(path).getLen();

    LOG.debug("Total tuple wrote {} File size {}", tuples, fsize / (1024.0 * 1024));
  }



  @Test
  public void seekDTFile() throws IOException
  {
    Random random = new Random();
    int ikey = random.nextInt(tuples);
    byte[] key = new byte[16];
    ByteBuffer bb = ByteBuffer.wrap(key);
    bb.putLong(ikey);

    FSDataInputStream fsdis = fs.open(path);

    if (CacheManager.getCache() != null) {
      CacheManager.getCache().invalidateAll();
    }
    CacheManager.setEnableStats(true);
    Assert.assertEquals("Cache Contains no block", CacheManager.getCacheSize(), 0);

    DTFile.Reader reader = new DTFile.Reader(fsdis, fs.getFileStatus(path).getLen(), conf);
    DTFile.Reader.Scanner scanner = reader.createScanner();

    /* Read first key in the file */
    scanner.lowerBound(key);
    Assert.assertTrue("Cache contains some blocks ", CacheManager.getCacheSize() > 0);

    /* Next key does not add a new block in cache, it reads directly from cache */
    // close scanner, so that it does not use its own cache.
    scanner.close();
    ikey++;
    bb.clear();
    bb.putLong(ikey);

    long numBlocks = CacheManager.getCacheSize();
    long hit = CacheManager.getCache().stats().hitCount();
    scanner.lowerBound(key);
    Assert.assertEquals("Cache contains some blocks ", CacheManager.getCacheSize(), numBlocks);
    Assert.assertEquals("Cache hit ", CacheManager.getCache().stats().hitCount(), hit + 1);

    /* test cache miss */
    scanner.close();
    hit = CacheManager.getCache().stats().hitCount();
    long oldmiss = CacheManager.getCache().stats().missCount();
    ikey = tuples - 1;
    bb.clear();
    bb.putLong(ikey);
    numBlocks = CacheManager.getCacheSize();
    scanner.lowerBound(key);
    Assert.assertEquals("Cache contains one more blocks ", CacheManager.getCacheSize(), numBlocks + 1);
    Assert.assertEquals("No cache hit ", CacheManager.getCache().stats().hitCount(), hit);
    Assert.assertEquals("Cache miss", CacheManager.getCache().stats().missCount(), oldmiss + 1);

    Assert.assertEquals("Reverse lookup cache and block cache has same number of entries",
        reader.readerBCF.getCacheKeys().size(), CacheManager.getCacheSize());
    reader.close();
    Assert.assertEquals("Cache blocks are deleted on reader close ", CacheManager.getCacheSize(), 0);
    Assert.assertEquals("Size of reverse lookup cache is zero ", 0, reader.readerBCF.getCacheKeys().size());
  }

  @Test
  public void checkInvalidKeys()
  {
    /* invalidating non existing key do not throw exception */
    List<String> lst = new LinkedList<String>();
    lst.add("One");
    lst.add("Two");
    CacheManager.getCache().invalidateAll(lst);
  }

  @Before
  public void createDTfile() throws IOException
  {
    setUp();
    writeTFile();
  }

  private static final Logger LOG = LoggerFactory.getLogger(DTFileTest.class);

}
