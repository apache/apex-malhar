package com.datatorrent.contrib.hbase;
/**
 * @displayName: Abstract HBase Transactional Put Output
 * @category: store
 * @tag: output, put, transactional
 * @deprecated
 * HBase operators are not truly transactional.It is only near transactional.
 * AbstractHBaseTransactionalPutOutputOperator is a misnomer.
 * Deprecated as of 1.0.4
 */
@Deprecated 
public abstract class AbstractHBaseTransactionalPutOutputOperator<T> extends AbstractHBaseWindowPutOutputOperator<T>
{

}
