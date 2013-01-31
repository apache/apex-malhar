/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.wordcount;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class WordCountZeroMQInputOperator implements InputOperator {

    private static final Logger logger = LoggerFactory.getLogger(WordCountZeroMQInputOperator.class);
    private Thread wordReadThread;
    private String zmqAddress = "tcp://localhost:5555";
    public transient DefaultOutputPort<String> output = new DefaultOutputPort<String>(this);

    @Override
    public void emitTuples() {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket socket = context.socket(ZMQ.PULL);

        System.out.println("Address: "+zmqAddress);
        socket.connect(zmqAddress);
        
        for (int i = 0; i < 10; i++) {
            String word = socket.recvStr();
            output.emit(word);
        }
    }

    @Override
    public void beginWindow(long windowId) {
        System.out.println("BEGIN WINDOW");
    }

    @Override
    public void endWindow() {
        System.out.println("END WINDOW");
    }

    @Override
    public void setup(OperatorContext c) {

        WordReadRun w = new WordReadRun();

        wordReadThread = new Thread(w);
        wordReadThread.start();
    }

    @Override
    public void teardown() {
        wordReadThread.interrupt();
    }
}
