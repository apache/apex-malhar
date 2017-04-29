This sample application show how to use POJOEnricher to enrich streaming data using
external source.
The operators in order as as follows:
1. Random data generator which emits data in JSON string format
2. JSON Parser which takes JSON string and emits POJO
3. POJO Enricher which enriches input using file and emits output POJO
4. Line Output Operator which emits line by line output to File System
The output files start out with a `.tmp` extension and get renamed when they reach the
size bound.

Similar to FSLoader JDBCLoader can be used when JDBC backend is required.


