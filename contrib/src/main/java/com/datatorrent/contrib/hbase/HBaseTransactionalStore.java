package com.datatorrent.contrib.hbase;
/**
 * @deprecated
 * HBase operators are not truly transactional.It is only near transactional.
 * HBaseTransactionalStore is a misnomer.
 */
@Deprecated 
public class HBaseTransactionalStore extends HBaseWindowStore
{

}
