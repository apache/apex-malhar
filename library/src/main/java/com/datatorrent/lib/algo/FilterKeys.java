package com.datatorrent.lib.algo;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Filters incoming tuples and emits the tuple that matches the filter condition for the key received.
 *
 * The key class has to implement FilterKeys.Filterable interface.
 * .
 * This is a pass through node<br>
 * <br>
 * <b>StateFull : No, </b> tuple are processed in current window. <br>
 * <b>Partitions : Yes, </b> no dependency among input tuples. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expect T (a POJO)<br>
 * <b>filter</b>: emits T (a POJO)<br>
 * <br>
 */
public class FilterKeys<K extends FilterKeys.Filterable, V> extends BaseOperator {

    public Filterable filter;

    /**
     * Interface which should be implemented by the Key class which is used in the data tuples.
     */
    public interface Filterable {

        /**
         * Allow the given key to be filtered or passed through.
         *
         * @param key
         * @return true if the given key is to be filtered or passed through, false otherwise.
         */
        boolean filter(Filterable key);
    }

    public transient DefaultInputPort<KeyValPair<K, V>> filterPort = new DefaultInputPort<KeyValPair<K, V>>() {
        @Override
        public void process(KeyValPair<K, V> tuple) {
            if (filter.filter(tuple.getKey())) {
                output.emit(tuple);
            }
        }
    };

    public transient DefaultOutputPort<KeyValPair<K, V>> output = new DefaultOutputPort<KeyValPair<K, V>>();

}
