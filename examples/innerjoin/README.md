Sample application to show how to use the POJOInnerJoinOperator to join two data streams.

Operators in sample application are as follows:
1) POJOGenerator which emits SalesEvent/ProductEvent POJO's
2) POJOInnerJoinOperator which joins two POJO streams and emits POJO tuple.
3) ConsoleOutputOperator which write joined POJO tuples to stdout.
