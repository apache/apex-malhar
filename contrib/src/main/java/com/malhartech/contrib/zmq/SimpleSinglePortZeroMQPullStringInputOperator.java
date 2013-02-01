/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.wordcount;

import com.malhartech.lib.io.SimpleSinglePortInputOperator;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class WordCountInputOperator extends SimpleSinglePortInputOperator<String> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(WordCountInputOperator.class);
    protected long averageSleep = 300;
    protected long sleepPlusMinus = 100;
    protected String fileName = "src/main/resources/com/malhartech/demos/wordcount/samplefile.txt";
    private transient BufferedReader br;
    private transient DataInputStream in;

    public void setAverageSleep(long as) {
        averageSleep = as;
    }

    public void setSleepPlusMinus(long spm) {
        sleepPlusMinus = spm;
    }

    public void setFileName(String fn) {
        fileName = fn;
    }

    @Override
    public void run() {
        while (true) {
            try {
                String line;
                FileInputStream fstream = new FileInputStream(fileName);

                in = new DataInputStream(fstream);
                br = new BufferedReader(new InputStreamReader(in));

                while ((line = br.readLine()) != null) {
                    String[] words = line.trim().split("[\\p{Punct}\\s\\\"\\'“”]+");
                    for (String word : words) {
                        word = word.trim().toLowerCase();
                        if (!word.isEmpty()) {
                            //System.out.println("Sending "+word);
                            outputPort.emit(word);
                        }
                    }
                    try {
                        Thread.sleep(averageSleep + (new Double(sleepPlusMinus * (Math.random() * 2 - 1))).longValue());
                    } catch (InterruptedException ex) {
                        
                    }
                }
                br.close();
                in.close();
                fstream.close();

            } catch (IOException ex) {
                logger.debug(ex.toString());
            }
        }
    }
}
