package com.datatorrent.examples.throttle;

import com.google.common.base.Throwables;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by pramod on 9/27/16.
 */
public class SlowDevNullOperator<T> extends BaseOperator {

    // Modify sleep time dynamically while app is running to increase and decrease sleep time
    long sleepTime = 1;

    public transient final DefaultInputPort<T> input = new DefaultInputPort<T>() {
        @Override
        public void process(T t) {
            // Introduce an artificial delay for every tuple
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }
    };

    public long getSleepTime() {
        return sleepTime;
    }

    public void setSleepTime(long sleepTime) {
        this.sleepTime = sleepTime;
    }
}
