Real-Time CTR Pipeline: High-Throughput Stream Processing Benchmark

This project implements and benchmarks a high-performance real-time data pipeline for calculating Ad Click-Through Rates (CTR). It simulates a production-grade streaming architecture to process tens of thousands of events per second. The project compares three distinct processing paradigms: a legacy SQL-based ingestion system (disk-bound) vs. modern in-memory distributed streaming engines (**Apache Spark Structured Streaming** and **Apache Flink**).

Project Structure & Key Components
----------------------------------

*   **producer.py**: The data generator. It simulates high-volume user traffic (Ad Views/Clicks) using configurable statistical distributions:
    
    *   **Poisson Process:** Simulates steady, average traffic.
        
    *   **MMPP (Markov-Modulated Poisson Process):** Simulates "bursty" traffic with intense spikes.
        
    *   It publishes raw events to the events Kafka topic.
        
*   **spark\_ctr\_job.py**: The Spark Structured Streaming engine. It performs a **Stream-Static Join** (enriching events with campaign data), executes windowed aggregations in memory, and handles late data using Watermarks. It outputs results to the ctr\_results topic.
    
*   **flink\_ctr\_job\_append.py**: The Apache Flink engine. It leverages the Flink Table API for continuous, pipelined processing. It calculates the same windowed analytics but uses an asynchronous, non-blocking architecture for ultra-low latency.
    
*   **latency\_analyzer.py**: The real-time dashboard. It consumes the final results from Kafka and calculates two critical performance metrics:
    
    1.  **Processing Latency ($L\_1$):** Time from event arrival at Kafka to calculation completion.
        
    2.  **Sink Latency ($L\_2$):** Time taken to publish results to the output topic.
        
*   **config.py**: Centralized configuration for Kafka servers, topic names, and window durations.
    
*   **reset\_topics.py**: A utility script to delete and recreate Kafka topics, ensuring a clean state for every experiment.
    

Prerequisites
-------------

Ensure you have the following installed:

*   **Python 3.8+**
    
*   **Java 17** (Required for Spark/Flink compatibility)
    
*   **Apache Kafka & Zookeeper** (Running locally or via Docker)
    
*   **Apache Spark** (v3.5+)
    
*   **Apache Flink** (v1.17+)
    

**Python Dependencies:**

Bash

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   pip install confluent-kafka pyspark apache-flink pandas matplotlib   `

1\. Running with Apache Spark
-----------------------------

Spark Structured Streaming operates in micro-batches. We optimize performance using a **Processing Time Trigger** to batch writes to Kafka.

Step 1: Start Infrastructure

Ensure Kafka and Zookeeper are running.

**Step 2: Reset Environment**

Bash

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   python reset_topics.py   `

Step 3: Start the Analyzer

Open a terminal to watch the results live:

Bash

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   python latency_analyzer.py   `

Step 4: Submit the Spark Job

You can run the job using spark-submit. Note: You must include the Kafka SQL package matching your Spark/Scala version.

Bash

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   # Example command (adjust package version as needed)  spark-submit \    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \    spark_ctr_job.py   `

_(Alternatively, use the provided helper script ./run\_spark\_job.sh)_

Step 5: Start the Data Stream

In a new terminal, start generating traffic:

Bash

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   python producer.py   `

2\. Running with Apache Flink
-----------------------------

Flink operates as a continuous streaming engine. We execute this using a local standalone cluster to simulate a distributed environment.

Step 1: Start Flink Cluster

Start the local Flink cluster (JobManager and TaskManager):

Bash

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   # Assuming FLINK_HOME is set  $FLINK_HOME/bin/start-cluster.sh   `

_Verify the dashboard at http://localhost:8081._

**Step 2: Reset Environment**

Bash

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   python reset_topics.py   `

**Step 3: Start the Analyzer**

Bash

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   python latency_analyzer.py   `

Step 4: Submit the Flink Job

Flink requires the Kafka connector JARs to be present in the classpath (usually $FLINK\_HOME/lib).

Bash

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   # Submit the job to the running cluster  $FLINK_HOME/bin/flink run \    -py flink_ctr_job_append.py   `

_(Alternatively, use the provided helper script ./run\_flink\_cluster.sh which handles environment variables automatically)._

**Step 5: Start the Data Stream**

Bash

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   python producer.py   `