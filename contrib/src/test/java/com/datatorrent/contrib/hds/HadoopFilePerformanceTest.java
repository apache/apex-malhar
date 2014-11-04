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
package com.datatorrent.contrib.hds;

import com.google.common.base.Stopwatch;

import org.junit.Assert;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.io.*;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner.Entry;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.MemoryMXBean;
import java.util.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.lang.management.ManagementFactory;

/**
 * Performance testing of various Hadoop files, including HFile, TFile, MapFile, Plain
 *
 * Tests include:
 *   - Sequential writes of keys in increasing sequence
 *   - Sequential reads of entire file
 *   - Sequential reads given a key id
 *   - Random reads given a key
 *   - Performance with compressed and uncompressed data
 *
 * Configurable parameters:
 *   - number of key/value pairs to test
 *   - characters used to generate values
 *   - size of each key ( default: 100 bytes )
 *   - size of each value ( default: 1000 bytes )
 *
 * Tests can be run in local mode or on Hadoop cluster.  To run tests on a Hadoop cluster use the following configuration:
 * Note: Be sure to replace MALHAR_PATH location with local checkout of <a href="https://github.com/DataTorrent/Malhar">Malhar</a> library.
 *
 *   MALHAR_PATH=~/repos/sashadt-malhar
 *   M2_HOME=/home/sasha/.m2
 *   export HADOOP_CLASSPATH="${M2_HOME}/repository/org/cloudera/htrace/htrace-core/2.04/htrace-core-2.04.jar:${M2_HOME}/repository/org/apache/hbase/hbase-protocol/0.98.2-hadoop2/hbase-protocol-0.98.2-hadoop2.jar:${M2_HOME}/repository/org/apache/hbase/hbase-client/0.98.2-hadoop2/hbase-client-0.98.2-hadoop2.jar:${M2_HOME}/repository/org/apache/hbase/hbase-common/0.98.2-hadoop2/hbase-common-0.98.2-hadoop2.jar:${M2_HOME}/repository/org/apache/hbase/hbase-server/0.98.2-hadoop2/hbase-server-0.98.2-hadoop2.jar:${MALHAR_PATH}/contrib/target/*"
 *   export HADOOP_CLIENT_OPTS="-DTEST_KV_COUNT=100000 -DTEST_KEY_SIZE_BYTES=100 -DTEST_VALUE_SIZE_BYTES=1000 -Dlog4j.debug -Dlog4j.configuration=file://${MALHAR_PATH}/library/src/test/resources/log4j.properties"
 *   hadoop org.junit.runner.JUnitCore com.datatorrent.contrib.hds.HadoopFilePerformanceTest
 *
 */
public class HadoopFilePerformanceTest {
  private static final Logger logger = LoggerFactory.getLogger(HadoopFilePerformanceTest.class);

  public HadoopFilePerformanceTest() {
  }

  private static int testSize = Integer.parseInt(System.getProperty("TEST_KV_COUNT", "10000"));
  private static int keySizeBytes = Integer.parseInt(System.getProperty("TEST_KEY_SIZE_BYTES", "100"));
  private static int valueSizeBytes = Integer.parseInt(System.getProperty("TEST_VALUE_SIZE_BYTES", "1000"));
  private static int blockSize = Integer.parseInt(System.getProperty("TEST_BLOCK_SIZE", "65536"));
  private static char[] valueValidChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
  private static String keyFormat = "%0" + keySizeBytes + "d";
  private static Map<String, String> testSummary = new TreeMap<String, String>();

  private static String getValue() {
    return RandomStringUtils.random(valueSizeBytes, valueValidChars);
  }
  private static String getKey(int i) {
    return String.format(keyFormat, i);
  }

  private enum Testfile {
    MAPFILE, TFILE, TFILE_GZ, DTFILE, DTFILE_GZ, HFILE, HFILE_GZ, PLAIN;
    public String filename() {
      return HadoopFilePerformanceTest.class.getName() + ".test." + this.toString();
    }
    public Path filepath() {
      return new Path(filename());
    }
  }


  private Configuration conf = null;
  private FileSystem hdfs = null;


  @Before
  public void startup() throws Exception {
    conf = new Configuration();
    hdfs = FileSystem.get(conf);
    deleteTestfiles();
  }

  @After
  public void cleanup() throws Exception {
    deleteTestfiles();
    hdfs.close();
  }

  private Stopwatch timer = new Stopwatch();

  private void startTimer() {
    timer.reset();
    timer.start();
  }

  private String stopTimer(Testfile fileType, String testType) throws IOException {
    timer.stop();
    long elapsedMS = timer.elapsedTime(TimeUnit.MICROSECONDS);
    String testKey = testSize + "," + fileType.name() + "-" + testType;
    long fileSize = hdfs.getContentSummary(fileType.filepath()).getSpaceConsumed();
    testSummary.put(testKey, "" + elapsedMS + "," + fileSize);
    return String.format("%,d", timer.elapsedTime(TimeUnit.NANOSECONDS)) + " ns ( " + timer.toString(6) + " )";
  }


  @AfterClass
  public static void summary() throws Exception {
    long heapMax = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    long nonHeapMax = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getMax();
    System.out.println("==============================================================================");
    System.out.println("Test Size: " + String.format("%,d", testSize) + " pairs (" + String.format("%,d", keySizeBytes) + " key bytes /"+String.format("%,d", valueSizeBytes)+" value bytes)");
    System.out.println("Memory: " + String.format("%,d", heapMax) + " Heap MAX +  " + String.format("%,d", nonHeapMax) + " Non-Heap Max =  " + String.format("%,d", (heapMax+nonHeapMax)) + " Total MAX");
    System.out.println("==============================================================================");
    System.out.println("KV PAIRS ("+keySizeBytes+"/"+valueSizeBytes+"), TEST ID, ELAPSED TIME (μs/microseconds), FILE SIZE (bytes)");
    Iterator it = testSummary.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry kv = (Map.Entry)it.next();
      System.out.println(kv.getKey() + "," + kv.getValue());
    }
  }


  private void deleteTestfiles() throws Exception {
    for (Testfile t: Testfile.values()) {
      if ( hdfs.exists( t.filepath() )) {
        logger.debug("deleting: {}", t.filename());
        hdfs.delete( t.filepath(), true );
      }
    }
  }


  @Test
  public void testPlainFileWrite() throws Exception {

    Path file = Testfile.PLAIN.filepath();

    logger.debug("Writing {} with {} key/value pairs", file, String.format("%,d", testSize));

    startTimer();
    FSDataOutputStream fos = hdfs.create(file);
    for (int i=0; i < testSize; i++) {
      fos.writeUTF(getKey(i) + ":" + getValue());
    }
    fos.close();
    logger.info("Duration: {}", stopTimer(Testfile.PLAIN, "WRITE"));
    logger.debug("Space consumed: {} bytes", String.format("%,d", hdfs.getContentSummary(file).getSpaceConsumed()));

    Assert.assertTrue(hdfs.exists(file));
    hdfs.delete( file, true );
  }


  private void writeMapFile() throws Exception {
    Path path = Testfile.MAPFILE.filepath();

    Text key = new Text();
    Text value = new Text();


    long fsMinBlockSize = conf.getLong("dfs.namenode.fs-limits.min-block-size", 0);

    long testBlockSize = ((long)blockSize < fsMinBlockSize ) ? fsMinBlockSize : (long)blockSize;

    MapFile.Writer writer =  new MapFile.Writer(conf, path,
            MapFile.Writer.keyClass(key.getClass()),
            MapFile.Writer.valueClass(value.getClass()),
            MapFile.Writer.compression(SequenceFile.CompressionType.NONE),
            SequenceFile.Writer.blockSize(testBlockSize),
            SequenceFile.Writer.bufferSize((int)testBlockSize));
    for (int i=0; i < testSize; i++) {
      key.set(getKey(i));
      value.set(getValue());
      writer.append(key, value);
    }
    IOUtils.closeStream(writer);
  }


  @Test
  public void testMapFileWrite() throws Exception {

    Path file = Testfile.MAPFILE.filepath();
    logger.debug("Writing {} with {} key/value pairs", file, String.format("%,d", testSize));

    startTimer();
    writeMapFile();
    logger.info("Duration: {}", stopTimer(Testfile.MAPFILE, "WRITE"));

    Assert.assertTrue(hdfs.exists(file));
    ContentSummary fileInfo = hdfs.getContentSummary(file);
    logger.debug("Space consumed: {} bytes in {} files",
            String.format("%,d", fileInfo.getSpaceConsumed()),
            String.format("%,d", fileInfo.getFileCount()));
  }

  @Test
  public void testMapFileRead() throws Exception {

    logger.info("Reading {} with {} key/value pairs", Testfile.MAPFILE.filename(), String.format("%,d", testSize));
    writeMapFile();

    Text key = new Text();
    Text value = new Text();

    writeMapFile();

    // Set amount of memory to use for buffer
    float bufferPercent = 0.25f;
    int bufferSize = (int)(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() * bufferPercent);

    MapFile.Reader reader = new MapFile.Reader(Testfile.MAPFILE.filepath(), conf, SequenceFile.Reader.bufferSize(bufferSize));

    startTimer();
    reader.reset();
    while (reader.next(key,value)) {
      //logger.debug("read key:{} value:{}", key, value);
    }
    logger.info ("Duration for reader.next() SEQUENTIAL keys: {}",  stopTimer(Testfile.MAPFILE, "READ-SEQ"));

    startTimer();
    reader.reset();
    for (int i=0; i < testSize; i++) {
      key.set(getKey(i));
      reader.get(key, value);
      //logger.debug("{}:{}", key, value);
    }
    logger.info ("Duration for reader.get(key) SEQUENTIAL keys: {}",  stopTimer(Testfile.MAPFILE, "READ-SEQ-ID"));

    Random random = new Random();
    startTimer();
    for (int i=0; i < testSize; i++) {
      key.set(getKey(random.nextInt(testSize)));
      reader.get(key,value);
      //logger.debug("{}:{}", key, value);
    }
    logger.info ("Duration for reader.get(key) RANDOM keys: {}",  stopTimer(Testfile.MAPFILE, "READ-RAND"));

  }



  public void writeHFile(Path file, Compression.Algorithm compression) throws Exception {

    CacheConfig cacheConf = new CacheConfig(conf);
    cacheConf.shouldEvictOnClose();
    FSDataOutputStream fos = hdfs.create(file);
    KeyValue.KVComparator comparator = new KeyValue.RawBytesComparator();

    HFileContext context = new HFileContextBuilder()
            .withBlockSize(blockSize)
            .withCompression(compression)
            .build();

    logger.debug("context.getBlockSize(): {}", context.getBlocksize());
    logger.debug("context.getCompression(): {}", context.getCompression());
    logger.debug("context.getDataBlockEncoding(): {}", context.getDataBlockEncoding());
    logger.debug("context.getBytesPerChecksum(): {}", context.getBytesPerChecksum());

    HFile.Writer writer = new HFileWriterV3(conf, cacheConf, hdfs, file, fos, comparator, context);

    for (int i=0; i < testSize; i++) {
      writer.append(getKey(i).getBytes(), getValue().getBytes());
      //logger.debug("fos.getPos(): {}", fos.getPos() );
    }

    //writer.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY, Bytes.toBytes(true));
    writer.close();
  }


  public void writeTFile(Path file, String cname) throws Exception {


    FSDataOutputStream fos = hdfs.create(file);

    TFile.Writer writer = new TFile.Writer(fos, blockSize, cname, "jclass:" + BytesWritable.Comparator.class.getName(), new Configuration());

    for (int i=0; i < testSize; i++) {
      String k = getKey(i);
      String v = getValue();
      writer.append(k.getBytes(), v.getBytes());
    }

    writer.close();
    fos.close();
  }

  @Test
  public void testHFileWrite() throws Exception {
    Path file = Testfile.HFILE.filepath();
    logger.info("Writing {} with {} key/value pairs", file, String.format("%,d", testSize));

    startTimer();
    writeHFile(file, Compression.Algorithm.NONE);
    logger.info ("Duration: {}",  stopTimer(Testfile.HFILE, "WRITE"));

    Assert.assertTrue(hdfs.exists(file));
    ContentSummary fileInfo = hdfs.getContentSummary(file);
    logger.debug("Space consumed: {} bytes in {} files",
            String.format("%,d", fileInfo.getSpaceConsumed()),
            String.format("%,d", fileInfo.getFileCount()));
  }

  @Test
  public void testHFileWriteGZ() throws Exception {
    Path file = Testfile.HFILE_GZ.filepath();
    logger.info("Writing {} with {} key/value pairs", file, String.format("%,d", testSize));

    startTimer();
    writeHFile(file, Compression.Algorithm.GZ);
    logger.info ("Duration: {}",  stopTimer(Testfile.HFILE_GZ, "WRITE"));

    Assert.assertTrue(hdfs.exists(file));
    ContentSummary fileInfo = hdfs.getContentSummary(file);
    logger.debug("Space consumed: {} bytes in {} files",
            String.format("%,d", fileInfo.getSpaceConsumed()),
            String.format("%,d", fileInfo.getFileCount()));
  }


  @Test
  public void testTFileWrite() throws Exception {
    Path file = Testfile.TFILE.filepath();
    logger.info("Writing {} with {} key/value pairs", file, String.format("%,d", testSize));

    startTimer();
    writeTFile(file, TFile.COMPRESSION_NONE);
    logger.info ("Duration: {}",  stopTimer(Testfile.TFILE, "WRITE"));

    Assert.assertTrue(hdfs.exists(file));
    ContentSummary fileInfo = hdfs.getContentSummary(file);
    logger.debug("Space consumed: {} bytes in {} files",
            String.format("%,d", fileInfo.getSpaceConsumed()),
            String.format("%,d", fileInfo.getFileCount()));
  }

  @Test
  public void testTFileWriteGZ() throws Exception {
    Path file = Testfile.TFILE_GZ.filepath();
    logger.info("Writing {} with {} key/value pairs", file, String.format("%,d", testSize));

    startTimer();
    writeTFile(file, TFile.COMPRESSION_GZ);
    logger.info ("Duration: {}",  stopTimer(Testfile.TFILE_GZ, "WRITE"));

    Assert.assertTrue(hdfs.exists(file));
    ContentSummary fileInfo = hdfs.getContentSummary(file);
    logger.debug("Space consumed: {} bytes in {} files",
            String.format("%,d", fileInfo.getSpaceConsumed()),
            String.format("%,d", fileInfo.getFileCount()));
  }

  private void readHFileSeq(Path file, Compression.Algorithm compression) throws Exception {

    CacheConfig cacheConf = new CacheConfig(conf);
    HFile.Reader reader = HFile.createReader(hdfs, file, cacheConf, conf);
    HFileScanner scanner = reader.getScanner(true, true, false);

    scanner.seekTo();

    KeyValue kv = null;
    while (scanner.next()) {
      kv = scanner.getKeyValue();
      //logger.debug("key: {} value: {}", new String (kv.getKey()), new String (kv.getValue()));
    }

  }

  private void readHFileSeqId(Path file, Compression.Algorithm compression) throws Exception {
    CacheConfig cacheConf = new CacheConfig(conf);
    HFile.Reader reader = HFile.createReader(hdfs, file, cacheConf, conf);
    HFileScanner scanner = reader.getScanner(true, true, false);

    KeyValue kv = null;
    scanner.seekTo();

    for (int i = 0; i < testSize; i++) {
      scanner.seekTo(getKey(i).getBytes());
      kv = scanner.getKeyValue();
      //logger.debug("key: {} value: {}", new String (kv.getKey()), new String (kv.getValue()));
    }
  }

  private void readHFileRandom(Path file, Compression.Algorithm compression) throws Exception {
    CacheConfig cacheConf = new CacheConfig(conf);
    HFile.Reader reader = HFile.createReader(hdfs, file, cacheConf, conf);
    HFileScanner scanner = reader.getScanner(true, true, false);

    KeyValue kv = null;
    scanner.seekTo();
    Random random = new Random();
    for (int i=0; i < testSize; i++) {
      scanner.seekTo();
      scanner.seekTo(getKey(random.nextInt(testSize)).getBytes());
      kv = scanner.getKeyValue();
      //logger.debug("key: {} value: {}", new String (kv.getKey()), new String (kv.getValue()));
    }
  }

  @Test
  public void testHFileRead() throws Exception {

    Path file = Testfile.HFILE.filepath();
    Compression.Algorithm compression = Compression.Algorithm.NONE;
    logger.info("Reading {} with {} key/value pairs", file, String.format("%,d", testSize));
    writeHFile(file, compression);

    startTimer();
    readHFileSeq(file, compression);
    logger.info("Duration for scanner.next() SEQUENTIAL keys: {}", stopTimer(Testfile.HFILE, "READ-SEQ"));

    startTimer();
    readHFileSeqId(file, compression);
    logger.info("Duration for scanner.seekTo(key) SEQUENTIAL keys: {}", stopTimer(Testfile.HFILE, "READ-SEQ-ID"));

    startTimer();
    readHFileRandom(file, compression);
    logger.info("Duration for scanner.seekTo(key) RANDOM keys: {}", stopTimer(Testfile.HFILE, "READ-RAND"));

  }

  @Test
  public void testHFileReadGZ() throws Exception {

    Path file = Testfile.HFILE_GZ.filepath();
    Compression.Algorithm compression = Compression.Algorithm.GZ;
    logger.info("Reading {} with {} key/value pairs", file, String.format("%,d", testSize));
    writeHFile(file, compression);

    startTimer();
    readHFileSeq(file, compression);
    logger.info("Duration for scanner.next() SEQUENTIAL keys: {}",stopTimer(Testfile.HFILE_GZ, "READ-SEQ"));

    startTimer();
    readHFileSeqId(file, compression);
    logger.info("Duration for scanner.seekTo(key) SEQUENTIAL keys: {}", stopTimer(Testfile.HFILE_GZ, "READ-SEQ-ID"));

    startTimer();
    readHFileRandom(file, compression);
    logger.info ("Duration for scanner.seekTo(key) RANDOM keys: {}",  stopTimer(Testfile.HFILE_GZ, "READ-RAND"));

  }


  @Test
  public void testTFileRead() throws Exception {

    Path file = Testfile.TFILE.filepath();
    logger.info("Reading {} with {} key/value pairs", file, String.format("%,d", testSize));
    writeTFile(file, TFile.COMPRESSION_NONE);

    startTimer();
    readTFileSeq(file);
    logger.info("Duration for scanner.next() SEQUENTIAL keys: {}", stopTimer(Testfile.TFILE, "READ-SEQ"));

    startTimer();
    readTFileSeqId(file);
    logger.info("Duration for scanner.seekTo(key) SEQUENTIAL keys: {}", stopTimer(Testfile.TFILE, "READ-SEQ-ID"));

    startTimer();
    readTFileRandom(file);
    logger.info("Duration for scanner.seekTo(key) RANDOM keys: {}", stopTimer(Testfile.TFILE, "READ-RAND"));

  }

  @Test
  public void testTFileReadGZ() throws Exception {

    Path file = Testfile.TFILE_GZ.filepath();
    logger.info("Reading {} with {} key/value pairs", file, String.format("%,d", testSize));
    writeTFile(file, TFile.COMPRESSION_GZ);

    startTimer();
    readTFileSeq(file);
    logger.info("Duration for scanner.next() SEQUENTIAL keys: {}", stopTimer(Testfile.TFILE_GZ, "READ-SEQ"));

    startTimer();
    readTFileSeqId(file);
    logger.info("Duration for scanner.seekTo(key) SEQUENTIAL keys: {}", stopTimer(Testfile.TFILE_GZ, "READ-SEQ-ID"));

    startTimer();
    readTFileRandom(file);
    logger.info ("Duration for scanner.seekTo(key) RANDOM keys: {}",  stopTimer(Testfile.TFILE_GZ, "READ-RAND"));

  }
  
  private void readTFileRandom(Path file) throws IOException
  {
    
    Random random = new Random();
    
    FSDataInputStream in = hdfs.open(file);
    long size = hdfs.getContentSummary(file).getLength();
    TFile.Reader reader = new TFile.Reader(in, size, new Configuration());
    Scanner scanner = reader.createScanner();
    scanner.rewind();
    
    for(int i=0; i< testSize; i++){
//      scanner.rewind();
      scanner.seekTo(getKey(random.nextInt(testSize)).getBytes());
//      Entry en = scanner.entry();
//      en.get(new BytesWritable(new byte[en.getKeyLength()]), new BytesWritable(new byte[en.getValueLength()]));
    }
    reader.close();


  }
  private void readTFileSeqId(Path file) throws IOException
  {
    
    FSDataInputStream in = hdfs.open(file);
    long size = hdfs.getContentSummary(file).getLength();
    TFile.Reader reader = new TFile.Reader(in, size, new Configuration());
    Scanner scanner = reader.createScanner();
    scanner.rewind();
    
    for(int i=0; i< testSize; i++){
      scanner.seekTo(getKey(i).getBytes());
      Entry en = scanner.entry();
      en.get(new BytesWritable(new byte[en.getKeyLength()]), new BytesWritable(new byte[en.getValueLength()]));
    }
    reader.close();
    
  }
  private void readTFileSeq(Path file) throws IOException
  {
    
    FSDataInputStream in = hdfs.open(file);
    long size = hdfs.getContentSummary(file).getLength();
    TFile.Reader reader = new TFile.Reader(in, size, new Configuration());
    Scanner scanner = reader.createScanner();
    scanner.rewind();
    do {
      Entry en = scanner.entry();
      en.get(new BytesWritable(new byte[en.getKeyLength()]), new BytesWritable(new byte[en.getValueLength()]));
    } while(scanner.advance() && !scanner.atEnd());
      
    reader.close();
    
  }
  
  
  
  
  @Test
  public void testDTFileRead() throws Exception {

    Path file = Testfile.DTFILE.filepath();
    logger.info("Reading {} with {} key/value pairs", file, String.format("%,d", testSize));
    writeTFile(file, TFile.COMPRESSION_NONE);

    startTimer();
    readDTFileSeq(file);
    logger.info("Duration for scanner.next() SEQUENTIAL keys: {}", stopTimer(Testfile.DTFILE, "READ-SEQ"));

    startTimer();
    readDTFileSeq(file);
    logger.info("Duration for scanner.seekTo(key) SEQUENTIAL keys: {}", stopTimer(Testfile.DTFILE, "READ-SEQ-ID"));

    startTimer();
    readDTFileRandom(file);
    logger.info("Duration for scanner.seekTo(key) RANDOM keys: {}", stopTimer(Testfile.DTFILE, "READ-RAND"));

  }

  @Test
  public void testDTFileReadGZ() throws Exception {

    Path file = Testfile.DTFILE_GZ.filepath();
    logger.info("Reading {} with {} key/value pairs", file, String.format("%,d", testSize));
    writeTFile(file, TFile.COMPRESSION_GZ);

    startTimer();
    readDTFileSeq(file);
    logger.info("Duration for scanner.next() SEQUENTIAL keys: {}", stopTimer(Testfile.DTFILE_GZ, "READ-SEQ"));

    startTimer();
    readDTFileSeqId(file);
    logger.info("Duration for scanner.seekTo(key) SEQUENTIAL keys: {}", stopTimer(Testfile.DTFILE_GZ, "READ-SEQ-ID"));

    startTimer();
    readDTFileRandom(file);
    logger.info ("Duration for scanner.seekTo(key) RANDOM keys: {}",  stopTimer(Testfile.DTFILE_GZ, "READ-RAND"));

  }
  
  
  private void readDTFileSeq(Path file) throws IOException
  {
    
    FSDataInputStream in = hdfs.open(file);
    long size = hdfs.getContentSummary(file).getLength();
    org.apache.hadoop.io.file.tfile.DTFile.Reader reader = new org.apache.hadoop.io.file.tfile.DTFile.Reader(in, size, new Configuration());
    org.apache.hadoop.io.file.tfile.DTFile.Reader.Scanner scanner = reader.createScanner();
    scanner.rewind();
    do {
      org.apache.hadoop.io.file.tfile.DTFile.Reader.Scanner.Entry en = scanner.entry();
      en.getBlockBuffer();
      en.getKeyOffset();
      en.getKeyLength();
      en.getValueLength();
      en.getValueOffset();
//    System.out.println(new String(Arrays.copyOfRange(en.getBlockBuffer(), en.getKeyOffset(), en.getKeyOffset() + en.getKeyLength())) + ", " + new String(Arrays.copyOfRange(en.getBlockBuffer(), en.getValueOffset(), en.getValueOffset() + en.getValueLength())));

    } while(scanner.advance() && !scanner.atEnd());
      
    reader.close();
    
  }
  
  private void readDTFileRandom(Path file) throws IOException
  {
    
    Random random = new Random();
    
    FSDataInputStream in = hdfs.open(file);
    long size = hdfs.getContentSummary(file).getLength();
    org.apache.hadoop.io.file.tfile.DTFile.Reader reader = new org.apache.hadoop.io.file.tfile.DTFile.Reader(in, size, new Configuration());
    org.apache.hadoop.io.file.tfile.DTFile.Reader.Scanner scanner = reader.createScanner();
    scanner.rewind();
    
    for(int i=0; i< testSize; i++){
      scanner.seekTo(getKey(random.nextInt(testSize)).getBytes());
      org.apache.hadoop.io.file.tfile.DTFile.Reader.Scanner.Entry en = scanner.entry();
      en.getBlockBuffer();
      en.getKeyOffset();
      en.getKeyLength();
      en.getValueLength();
      en.getValueOffset();
//      System.out.println(new String(Arrays.copyOfRange(en.getBlockBuffer(), en.getKeyOffset(), en.getKeyOffset() + en.getKeyLength())) + ", " + new String(Arrays.copyOfRange(en.getBlockBuffer(), en.getValueOffset(), en.getValueOffset() + en.getValueLength())));
      
    }
    reader.close();


  }
  private void readDTFileSeqId(Path file) throws IOException
  {
    
    FSDataInputStream in = hdfs.open(file);
    long size = hdfs.getContentSummary(file).getLength();
    org.apache.hadoop.io.file.tfile.DTFile.Reader reader = new org.apache.hadoop.io.file.tfile.DTFile.Reader(in, size, new Configuration());
    org.apache.hadoop.io.file.tfile.DTFile.Reader.Scanner scanner = reader.createScanner();
    scanner.rewind();
    
    for(int i=0; i< testSize; i++){
      scanner.seekTo(getKey(i).getBytes());
      org.apache.hadoop.io.file.tfile.DTFile.Reader.Scanner.Entry en = scanner.entry();
      en.getBlockBuffer();
      en.getKeyOffset();
      en.getKeyLength();
      en.getValueLength();
      en.getValueOffset();
//    System.out.println(new String(Arrays.copyOfRange(en.getBlockBuffer(), en.getKeyOffset(), en.getKeyOffset() + en.getKeyLength())) + ", " + new String(Arrays.copyOfRange(en.getBlockBuffer(), en.getValueOffset(), en.getValueOffset() + en.getValueLength())));

    }
    reader.close();
    
  }

  


}
