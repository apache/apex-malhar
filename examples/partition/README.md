This example shows how to define custom partitions and a custom `StreamCodec` to customize
the set of tuples that reach each partition.

There are two operators: `RandomNumberGenerator` (generates random integers) and
`TestPartition` (logs input tuples).

The application also uses a StreamCodec called `Codec3` to tag each tuple with a
partition tag based on whether the number is divisible by 2 or 4.

`TestPartition` has code to create 3 partitions: one gets odd numbers, one gets multiples
of 4 and the last gets the rest. The `PartitionKeys` associated with each partition use
the partition tag to select the set of tuples to be handled by that partition.
