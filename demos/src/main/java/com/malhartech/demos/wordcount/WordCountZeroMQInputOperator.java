/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.wordcount;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.io.SimpleSinglePortInputOperator;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class WordCountZeroMQInputOperator extends SimpleSinglePortInputOperator<String> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(WordCountZeroMQInputOperator.class);
    private transient Thread wordReadThread;
    private String zmqAddress = "tcp://127.0.0.1:5555";
    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>(this);
    protected long averageSleep = 300;
    protected long sleepPlusMinus = 100;
    protected String fileName = "src/main/resources/com/malhartech/demos/wordcount/samplefile.txt";
    private transient BufferedReader br;
    private transient DataInputStream in;

    @Override
    public void emitTuples() {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket socket = context.socket(ZMQ.PULL);

        socket.connect(zmqAddress);
        int i = 0;
        while (true) {
            byte[] buf = socket.recv(0);
            if (buf == null) {
                //System.out.println("RECV: NO MORE DATA");
                break;
            }
            String word = "";
            for (int j = 0; j < buf.length; j++) {
                word += (char) buf[j];
            }
            //System.out.println("Emitting "+word);
            output.emit(word);
            if (++i > 100) {
                break;
            }
        }
        socket.close();
    }

    @Override
    public void beginWindow(long windowId) {
        //System.out.println("BEGIN WINDOW " + windowId);
    }

    @Override
    public void endWindow() {
        //System.out.println("END WINDOW");
    }

    @Override
    public void setup(OperatorContext c) {
    }

    @Override
    public void teardown() {
        wordReadThread.interrupt();
    }

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
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket socket = context.socket(ZMQ.PUSH);

        socket.bind(zmqAddress);

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
                            if (!socket.send(word)) {
                                throw new IOException("Cannot send to ZeroMQ Socket");
                            }
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
