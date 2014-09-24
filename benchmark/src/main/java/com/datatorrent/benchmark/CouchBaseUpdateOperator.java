package com.datatorrent.benchmark;

import com.datatorrent.contrib.couchbase.AbstractUpdateCouchBaseOutputOperator;

/**
 *
 * @author prerna
 */
public class CouchBaseUpdateOperator extends AbstractUpdateCouchBaseOutputOperator<Integer> {

    @Override
    public String generatekey(Integer tuple) {
        return "abc";
    }

    @Override
    public Object getObject(Integer tuple) {
        tuple = 500;
        return tuple;
    }

}
