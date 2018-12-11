package com.datatorrent.contrib.hbase;
/**
 * @displayName Abstract HBase NonTransactional Append Output
 * @category Store
 * @tags output operator, append
 * @deprecated
 * HBase operators are not truly transactional.It is only near transactional.
 * AbstractHBaseNonTransactionalAppendOutputOperator is a misnomer.
 * Deprecated as of 1.0.4
 */


@Deprecated 
public abstract class AbstractHBaseNonTransactionalAppendOutputOperator<T> extends AbstractHBaseAppendOutputOperator<T>
{

}
