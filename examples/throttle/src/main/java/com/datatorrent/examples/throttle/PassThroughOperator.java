package com.datatorrent.examples.throttle;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by pramod on 9/27/16.
 */
public class PassThroughOperator<T> extends BaseOperator {

    public transient final DefaultInputPort<T> input = new DefaultInputPort<T>() {
        @Override
        public void process(T t) {
            output.emit(t);
        }
    };

    public transient final DefaultOutputPort<T> output = new DefaultOutputPort<>();
}
