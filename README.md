Contains Spark Structured Streaming sources using V2 APIs
* Wiki edits
* TWitter tweets
* Some examples

Build steps:
* sbt gen-idea
* sbt package
* sbt assembly : to generate deployable jars

Apache Spark Datasource V2
* Checkout JIRA ticket https://issues.apache.org/jira/browse/SPARK-15689
* Understand the MicroBatchReader interface
* Good built-in V2 sources to study
  * RateStreamMicroBatchReader
  * TextSocketMicroBatchReader

Reference:
* http://blog.madhukaraphatak.com/migrate-spark-datasource-2.4/
* https://github.com/phatak-dev/spark2.0-examples/commit/1cfba393a09c2d4a012f29f7a6579ff5efc1dd53
