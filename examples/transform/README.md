Sample application to show how to use the TransformOperator to transform the input POJO using given expressions.

Operators in sample application are as follows:
1) POJOGenerator which generates and emits the CustomerEvent POJO.
2) TransformOperator which transforms the input POJO using provided expressions and emits the transformed POJO.
3) ConsoleOutputOperator which writes transformed POJO to stdout.
