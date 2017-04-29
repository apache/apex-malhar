This example illustrates the use of dynamic partitioning of an operator.

It uses an input operator that generates random numbers and outputs them to a
`DevNull` operator (which, as the name suggests, simply discards them).

The input operator starts out with two partitions; after some tuples have been
processed, a dynamic repartition is triggered via the `StatsListener` interface
to increase the number of partitions to four.
