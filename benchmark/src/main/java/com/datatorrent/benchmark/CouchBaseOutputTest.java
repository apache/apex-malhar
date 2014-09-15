
package com.datatorrent.benchmark;

/**
 *
 * @author prerna
 */


public class CouchBaseOutputTest {
    public static void main(String[] args){
    CouchBaseOutputOperator couchbaseOutputTest = new CouchBaseOutputOperator();
        couchbaseOutputTest.getStore().setBucket("default");
        couchbaseOutputTest.getStore().setPassword("");
        couchbaseOutputTest.getStore().setUriString("node26.morado.com:8091");
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
         couchbaseOutputTest.insertOrUpdate(i);
         }
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println(elapsedTime);
    }
    
}
