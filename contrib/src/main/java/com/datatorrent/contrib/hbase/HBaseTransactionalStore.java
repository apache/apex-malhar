package com.datatorrent.contrib.hbase;
/**
 * @deprecated
 * HBase operators are not truly transactional.It is only near transactional.
 * HBaseTransactionalStore is a misnomer.
 * Deprecated as of 1.0.4
 */
@Deprecated 
public class HBaseTransactionalStore extends HBaseWindowStore
{

}
