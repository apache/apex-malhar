package com.datatorrent.benchmark;

import com.datatorrent.contrib.couchbase.AbstractUpdateCouchBaseOutputOperator;

/**
 *
 * @author prerna
 */
public class CouchBaseUpdateOperator extends AbstractUpdateCouchBaseOutputOperator<String> {

    @Override
    public String generateKey(String tuple) {
        return "abc";
    }

    @Override
    public Object getObject(String tuple) {
        tuple = "500";
        return tuple;
    }

}
