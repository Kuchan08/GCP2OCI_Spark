
# Introduction
### Establish multicloud connectivity with OCI
With the rapidly growing popularity of Oracle Cloud Infrastructure, many customers want to migrate to OCI or use OCI as their multicloud solution. Likewise, many customers want to access other Cloud Data platforms from OCI and use OCI for processing/computing when dealing with Big Data solutions.

# Objective
This tutorial will demonstrate how to connect Google Cloud BigQuery from OCI Data Flow Spark Notebook and perform some read operation on BigQuery table using Spark. We will also cover how to write down the resultant Spark dataframe to OCI Object Storage and Autonomous Data Warehouse.

![GCP_BigQuery_To_OC_ADW](https://github.com/Kuchan08/OCI-DataFlow-/assets/54794358/cdf37368-682b-4a49-abb7-8310e2b06de9)


# Solution
This solution will leverage Apache Spark capability like parallel processing and distributed in memory computation. OCI Data Flow application also can be scheduled/orchestrated through OCI Data Integration Service. In this approach, user can develop their Spark Script on OCI Data Flow and Interactive Notebook which itself leverages OCI Data Flow Spark cluster. The high level steps are:

  1. Connect with Google Cloud Platform: Google Cloud BigQuery using Apache Spark BigQuery Connector.
  2. Develop complete ETL Solution.
  3. Extract Data from Google Cloud BigQuery.
  4. Transform the data using Apache Spark Cluster on OCI Data Flow.
  5. Ingest data in OCI Object Storage or Autonomous Data Warehouse.
  6. Use Developer’s friendly Interactive Spark Notebook.
  7. Integrate any supported open source Spark packages.
  8. Orchestrate your script using OCI Data Integration Service.

## Prerequisites
  1. An active OCI and Google Cloud subscription with access to the portal.

  2. Setup OCI Data Flow, OCI Object Storage Bucket and OCI Data Science Notebook. For more information, see:

      [OCI Data Flow Integration with OCI Data Science Notebook.](https://docs.oracle.com/en-us/iaas/data-flow/using/data-flow-studio.htm#data-flow-studio)

      [OCI Data Flow Integration with OCI Data Science Notebook.](https://blogs.oracle.com/ai-and-datascience/post/oci-dataflow-with-interactive-data-science-notebook)

  3. Create and download Google API JSON Key secret OCID for the project where BigQuery database is residing on Google Cloud.

  4. Upload the Google API JSON Key secret OCID to OCI Object Storage.

     Sample OCI Object Storage:
         oci://demo-bucketname@OSnamespace/gcp_utility/BigQuery/ocigcp_user_creds.json
     
  5. Download Spark BigQuery Jar and upload it to Object Storage.

     Sample: spark-bigquery-with-dependencies_2.12-0.23.2.jar

       [Download Spark BigQuery Jar.](https://mvnrepository.com/artifact/com.google.cloud.spark/spark-bigquery-with-dependencies_2.12/0.23.0)

  6. Collect the following parameters for your Google Cloud BigQuery table.

            'project' : 'bigquery-public-data'
         
            'parentProject' : 'core-invention-366213'
    
            'table' : 'bitcoin_blockchain.transactions'
    
            "credentialsFile" : "./ocigcp_user_creds.json"
     
  8. Download Autonomous Data Warehouse Wallet from the OCI Portal and keep the User/Password details handy.
