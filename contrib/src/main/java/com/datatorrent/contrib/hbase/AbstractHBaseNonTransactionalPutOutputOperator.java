package com.datatorrent.contrib.hbase;
/**
 * @deprecated
 * HBase operators are not truly transactional.It is only near transactional.
 * AbstractHBaseNonTransactionalPutOutputOperator is a misnomer.
 * Deprecated as of 1.0.4
 */
@Deprecated 
public abstract class AbstractHBaseNonTransactionalPutOutputOperator<T> extends AbstractHBasePutOutputOperator<T>
{

}
