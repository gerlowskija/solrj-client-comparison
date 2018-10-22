# SolrJ Client Comparison

A small driver program that I wrote to do some research for a talk at [Activate 2018](https://activate-conf.com).  (For more context see the slides for this talk, available [here](https://docs.google.com/presentation/d/1yoWL-4o5fITWNj-vpNDzdXFMlJtVFCGvbgo5asRHsD0/edit?usp=sharing)).

This driver performs a number of indexing runs using different indexing batch sizes and SolrClient implementations.  It is very basic, but serves as a reasonable starting point for learning about how a variety of factors affect SolrJ performance.  See the Javadocs on `SolrJBatchTester.java` for more details and caveats
