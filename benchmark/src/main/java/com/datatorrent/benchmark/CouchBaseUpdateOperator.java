package com.datatorrent.benchmark;

import com.datatorrent.contrib.couchbase.AbstractUpdateCouchBaseOutputOperator;

public class CouchBaseUpdateOperator extends AbstractUpdateCouchBaseOutputOperator<Integer> {

    @Override
    public String generateKey(Integer tuple) {
        return "yes" + tuple;
    }

    @Override
    public Object getObject(Integer tuple) {
        tuple = 500;
        return tuple;
    }

}
