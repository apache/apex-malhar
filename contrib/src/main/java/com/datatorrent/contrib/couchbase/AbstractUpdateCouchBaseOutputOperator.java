package com.datatorrent.contrib.couchbase;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author prerna
 */
public abstract class AbstractUpdateCouchBaseOutputOperator<T> extends AbstractCouchBaseOutputOperator<T> {

    private static final transient Logger logger = LoggerFactory.getLogger(AbstractUpdateCouchBaseOutputOperator.class);
    private int expireTime;

    @Override
    public void insertOrUpdate(T input) {
        String key = generatekey(input);
        Object tuple = getObject(input);
        ObjectMapper mapper = new ObjectMapper();
        String value = new String();
        try {
            value = mapper.writeValueAsString(tuple);
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(AbstractInsertCouchBaseOutputOperator.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            store.getInstance().add(key, this.expireTime, value).get();
        } catch (InterruptedException ex) {
            java.util.logging.Logger.getLogger(AbstractInsertCouchBaseOutputOperator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ExecutionException ex) {
            java.util.logging.Logger.getLogger(AbstractInsertCouchBaseOutputOperator.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public int getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(int expireTime) {
        this.expireTime = expireTime;
    }

}
