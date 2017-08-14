Solr:
====
Solr is a search platform from apache Lucene.  Its major features include powerful full-text search, hit highlighting, faceted search, near real-time indexing, dynamic clustering, database integration, rich document (e.g., Word, PDF) handling, and geospatial search. Solr is highly reliable, scalable and fault tolerant, providing distributed indexing, replication and load-balanced querying, automated failover and recovery and more.

Solr with Apex:
=====================
Solr search platform can be intergrated with Apex for variety of purposes for use cases ranging from simple keyword search to ranking, scoring, classification etc.

Solr InputOperator:
==================
AbstractSolrInputOperator class takes care of connecting to Solr server and querying it for data during each emitTuples() call. To implement input operator extend AbstractSolrInputOperator and 
implement following functions

1. initializeSolrServerConnector: Refer section SolrServer Initialization.
2. emitTuple: SolrServer returns data in form of SolrDocument override this method to convert the documents in the POJO objects as per your need.
3. getQueryParams: Write your search queries which will be invoked during eact emitTuples call to fetch data from Solr Server.

Solr OutputOperator:
===================
AbstractSolrOutputOperator class takes care of connecting to Solr server and pushing documents to server. To implement output operator extend AbstractSolrOutputOperator and implement following
functions
1. initializeSolrServerConnector: Refer section SolrServer Initialization.
2. getTuple: Data can flow in any format (e.g. POJO objects) override this method to convert incoming data to SolrInputDocument object so that it could be stored and indexed by Solr.


SolrServer Initialization:
=========================
Solr supports different server implementations like HttpSolrServer, CloudSolrServer, ConcurrentUpdateSolrServer and LBHttpSolrServer. Also it exposes relevant properties on each of these servers.
So SolrServer instance needs to be initialized as per business needs, by whoever want to use these operators.
HttpSolrServerConnector, wrapper over HttpSolrServer, is already in place and can be initialized by providing intialization parameters from dt-site.xml




