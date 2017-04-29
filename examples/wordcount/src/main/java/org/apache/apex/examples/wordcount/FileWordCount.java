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
package org.apache.apex.examples.wordcount;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * Computes word frequencies per file and globally, and writes the top N pairs to an output file
 * and to snapshot servers for visualization.
 * Currently designed to work with only 1 file at a time; will be enhanced later to support
 * multiple files dropped into the monitored directory at the same time.
 *
 * <p>
 * Receives per-window list of pairs (word, frequency) on the input port. When the end of a file
 * is reached, expects to get an EOF on the control port; at the next endWindow, the top N words
 * and frequencies are computed and emitted to the output ports.
 * <p>
 * There are 3 output ports: (a) One for the per-file top N counts emitted when the file is fully
 * read and is written to the output file. (b) One for the top N counts emitted per window for the
 * current file to the snapshot server and (c) One for the global top N counts emitted per window
 * to a different snapshot server.
 *
 * Since the EOF is received by a single operator, this operator cannot be partitionable
 *
 * @since 3.2.0
 */
public class FileWordCount extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(FileWordCount.class);
  private static final String GLOBAL = "global";

  /**
   * If {@literal topN > 0}, only data for the topN most frequent words is output; if topN == 0, the
   * entire frequency map is output
   */
  protected int topN;

  /**
   * Set to true when an EOF control tuple for the current input file is received; reset to false
   * when the corresponding output file has been written.
   */
  protected boolean eof = false;

  /**
   * Last component of path (just the file name)
   * incoming value from control tuple
   */
  protected String fileName;

  /**
   * {@literal (word => frequency)} map: current file, all words
   */
  protected Map<String, WCPair> wordMapFile = new HashMap<>();

  /**
   * {@literal (word => frequency)} map: global, all words
   */
  protected Map<String, WCPair> wordMapGlobal = new HashMap<>();

  /**
   * Singleton list with per file data; sent on {@code outputPerFile}
   */
  protected transient List<Map<String, Object>> resultPerFile;

  /**
   * Singleton list with global data; sent on {@code outputGlobal}
   */
  protected transient List<Map<String, Object>> resultGlobal;

  /**
   * Singleton map of {@code fileName} to sorted list of (word, frequency) pairs
   */
  protected transient Map<String, Object> resultFileFinal;

  /**
   * final list of (word, frequency) pairs written to output file
   */
  protected transient List<WCPair> fileFinalList;

  /**
   * Input port on which per-window {@literal (word => frequency)} map is received; the map
   * is merged into {@code wordMapFile} and {@code wordMapGlobal}.
   */
  public final transient DefaultInputPort<List<WCPair>> input = new DefaultInputPort<List<WCPair>>()
  {
    @Override
    public void process(List<WCPair> list)
    {
      // blend incoming list into wordMapFile and wordMapGlobal
      for (WCPair pair : list) {
        final String word = pair.word;
        WCPair filePair = wordMapFile.get(word);
        if (null != filePair) {    // word seen previously in current file
          WCPair globalPair = wordMapGlobal.get(word);    // cannot be null
          filePair.freq += pair.freq;
          globalPair.freq += pair.freq;
          continue;
        }

        // new word in current file
        filePair = new WCPair(word, pair.freq);
        wordMapFile.put(word, filePair);

        // check global map
        WCPair globalPair = wordMapGlobal.get(word);    // may be null
        if (null != globalPair) {    // word seen previously
          globalPair.freq += pair.freq;
          continue;
        }

        // word never seen before
        globalPair = new WCPair(word, pair.freq);
        wordMapGlobal.put(word, globalPair);
      }
    }
  };

  /**
   * Control port on which the current file name is received to indicate EOF
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<String> control = new DefaultInputPort<String>()
  {
    @Override
    public void process(String msg)
    {
      if (msg.isEmpty()) {    // sanity check
        throw new RuntimeException("Empty file path");
      }
      LOG.info("FileWordCount: EOF for {}, topN = {}", msg, topN);
      fileName = msg;
      eof = true;
      // NOTE: current version only supports processing one file at a time.
    }
  };

  /**
   * Output port for current file output
   */
  public final transient DefaultOutputPort<List<Map<String, Object>>>
      outputPerFile = new DefaultOutputPort<>();

  /**
   * Output port for global output
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<List<Map<String, Object>>>
      outputGlobal = new DefaultOutputPort<>();

  /**
   * Tuple is singleton map {@code fileName => TopNMap} where {@code TopNMap} is the final
   * top N pairs for current file and will be written to the output file; emitted in the
   * {@code endWindow()} call after an EOF
   */
  public final transient DefaultOutputPort<Map<String, Object>>
      fileOutput = new DefaultOutputPort<>();

  /**
   * Get the number of top (word, frequency) pairs that will be output
   */
  public int getTopN()
  {
    return topN;
  }

  /**
   * Set the number of top (word, frequency) pairs that will be output
   * @param n The new number
   */
  public void setTopN(int n)
  {
    topN = n;
  }

  /**
   * {@inheritDoc}
   * Initialize various map and list fields
   */
  @Override
  public void setup(OperatorContext context)
  {
    if (null == wordMapFile) {
      wordMapFile = new HashMap<>();
    }
    if (null == wordMapGlobal) {
      wordMapGlobal = new HashMap<>();
    }
    resultPerFile = new ArrayList(1);
    resultGlobal = new ArrayList(1);
    // singleton map {<fileName> => fileFinalList}; cannot populate it yet since we need fileName
    resultFileFinal = new HashMap<>(1);
    fileFinalList = new ArrayList<>();
  }

  /**
   * {@inheritDoc}
   * This is where we do most of the work:
   * 1. Sort global map and emit top N pairs
   * 2. Sort current file map and emit top N pairs
   * 3. If we've seen EOF, emit top N pairs on port connected to file writer and clear all per-file
   *    data structures.
   */
  @Override
  public void endWindow()
  {
    LOG.info("FileWordCount: endWindow for {}, topN = {}", fileName, topN);

    if (wordMapFile.isEmpty()) {    // no words found
      if (eof) {                    // write empty list to fileOutput port
        // got EOF, so output empty list to output file
        fileFinalList.clear();
        resultFileFinal.put(fileName, fileFinalList);
        fileOutput.emit(resultFileFinal);

        // reset for next file
        eof = false;
        fileName = null;
        resultFileFinal.clear();
      }
      LOG.info("FileWordCount: endWindow for {}, no words, topN = {}", fileName, topN);
      return;
    }

    LOG.info("FileWordCount: endWindow for {}, wordMapFile.size = {}, topN = {}", fileName, wordMapFile.size(), topN);

    // get topN list for this file and, if we have EOF, emit to fileOutput port

    // get topN global list and emit to global output port
    getTopNMap(wordMapGlobal, resultGlobal);
    LOG.info("FileWordCount: resultGlobal.size = {}", resultGlobal.size());
    outputGlobal.emit(resultGlobal);

    // get topN list for this file and emit to file output port
    getTopNMap(wordMapFile, resultPerFile);
    LOG.info("FileWordCount: resultPerFile.size = {}", resultPerFile.size());
    outputPerFile.emit(resultPerFile);

    if (eof) {                     // got EOF earlier
      if (null == fileName) {      // need file name to emit topN pairs to file writer
        throw new RuntimeException("EOF but no fileName at endWindow");
      }

      // so compute final topN list from wordMapFile into fileFinalList and emit it
      getTopNList(wordMapFile);
      resultFileFinal.put(fileName, fileFinalList);
      fileOutput.emit(resultFileFinal);

      // reset for next file
      eof = false;
      fileName = null;
      wordMapFile.clear();
      resultFileFinal.clear();
    }
  }

  /**
   * Get topN frequencies from map, convert each pair to a singleton map and append to result
   * This map is suitable input to AppDataSnapshotServer
   * MUST have {@code map.size() > 0} here
   */
  private void getTopNMap(final Map<String, WCPair> map, List<Map<String, Object>> result)
  {
    final ArrayList<WCPair> list = new ArrayList<>(map.values());

    // sort entries in descending order of frequency
    Collections.sort(list, new Comparator<WCPair>()
    {
      @Override
      public int compare(WCPair o1, WCPair o2)
      {
        return (int)(o2.freq - o1.freq);
      }
    });

    if (topN > 0) {
      list.subList(topN, map.size()).clear();      // retain only the first topN entries
    }

    // convert each pair (word, freq) of list to a map with 2 elements
    // {("word": <word>, "count": freq)} and append to list
    //
    result.clear();
    for (WCPair pair : list) {
      Map<String, Object> wmap = new HashMap<>(2);
      wmap.put("word", pair.word);
      wmap.put("count", pair.freq);
      result.add(wmap);
    }
    LOG.info("FileWordCount:getTopNMap: result.size = {}", result.size());
    list.clear();
  }

  /**
   * Populate fileFinalList with topN frequencies from argument
   * This list is suitable input to WordCountWriter which writes it to a file
   * MUST have {@code map.size() > 0} here
   */
  private void getTopNList(final Map<String, WCPair> map)
  {
    fileFinalList.clear();
    fileFinalList.addAll(map.values());

    // sort entries in descending order of frequency
    Collections.sort(fileFinalList, new Comparator<WCPair>()
    {
      @Override
      public int compare(WCPair o1, WCPair o2)
      {
        return (int)(o2.freq - o1.freq);
      }
    });

    if (topN > 0) {
      fileFinalList.subList(topN, map.size()).clear();      // retain only the first topN entries
    }
    LOG.info("FileWordCount:getTopNList: fileFinalList.size = {}", fileFinalList.size());
  }
}
