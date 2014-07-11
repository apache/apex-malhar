package com.datatorrent.benchmark;


import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datatorrent.contrib.cassandra.AbstractCassandraTransactionableOutputOperatorPS;


public class CassandraOutputOperator extends  AbstractCassandraTransactionableOutputOperatorPS<Integer>{

	private int id = 0;

	@Override
	protected PreparedStatement getUpdateCommand() {
		String statement = "Insert into test.cassandra_operator(id, result) values (?,?);";
		return store.getSession().prepare(statement);
	}

	@Override
	protected Statement setStatementParameters(PreparedStatement updateCommand,
			Integer tuple) throws DriverException {
		BoundStatement boundStmnt = new BoundStatement(updateCommand);
		return boundStmnt.bind(id++,tuple);
	}

}
