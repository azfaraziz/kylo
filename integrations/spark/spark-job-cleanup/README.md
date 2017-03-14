Spark Cleanup Job
==========

### Overview
A Spark job capable of performing cleanup of staging/temporary directories used by the standard-ingest Nifi template.

### How it works
This Spark job assumes Hive tables and directory naming conventions following standard-ingest processing standards.

1. Attributes from Nifi are passed to the Spark job via command line arguments
2. The \<feed>\_feed table is queried to get all job partitions (i.e. feedts/processing_dttm)
3. A low watermark is calculated using the **cleanup.retention.period** Nifi attribute. The default value for this is 7 days
4. Subset of all partitions are determined based on the low watermark
5. All hive staging/temp tables with partitions less than the watermark are removed
6. All HDFS/Local staging/temp directories with partitions less than the watermark are removed

The attributes are supplied externally via a JSON file.

### Execution

The project is a spark-submit job:

**Spark 1:**
```
./bin/spark-submit \
--master yarn-client \
--class com.thinkbiganalytics.spark.cleanup.Cleanup \
/path/to/jar/kylo-spark-job-cleanup-spark-v1-<version>-jar-with-dependencies.jar \
</path/to/attributes/file.json>
```

**Spark 2:**
```
./bin/spark-submit \
--master yarn-client \
--class com.thinkbiganalytics.spark.cleanup.Cleanup \
/path/to/jar/kylo-spark-job-cleanup-spark-v2-<version>-SNAPSHOT-jar-with-dependencies.jar \
</path/to/attributes/file.json>
```

### Build ###
mvn clean install package


### Example Attributes file (JSON)
The attributes JSON file is created using the **AttributesToJSON** Nifi processor. How to add this to the standard-ingest template is mentioned below.

```javascript

{
    "hive.profile.root": "/model.db",
    "spark.input_folder": "/tmp/kylo-nifi/spark",
    "merge.bin.age": "0",
    "feedts": "1488467463898",
    "path": "./",
    "metadata.table.targetTblProperties": "tblproperties(\"orc.compress\"=\"SNAPPY\")",
    "feedId": "4ad708cc-ff7b-4919-b744-4811cc9bb8cd",
    "kylo.tmp.baseFolder": "/tmp/kylo-nifi",
    "table_field_policy_json_file": "/tmp/kylo-nifi/spark/toy_store/products/1488467463898/products_field_policy.json",
    "metadata.table.partitionStructure": "",
    "hive.ingest.root": "/model.db",
    "hive.master.root": "/app/warehouse",
    "metadata.table.fieldIndexString": "productCode,productName,productVendor,productDescription,buyPrice",
    "metadata.table.targetFormat": "STORED AS ORC",
    "source.record.count": "110",
    "merge.correlation": "toy_store.products.correlation",
    "mime.type": "application/octet-stream",
    "skipHeader": "true",
    "metadata.table.partitionSpecs": "",
    "feed": "products",
    "hdfs.ingest.root": "/etl",
    "filename": "6288318909339",
    "water.mark": "1970-01-01T00:00:00",
    "activeWaterMarks": "{\"highWaterMark\":\"water.mark\"}",
    "category": "toy_store",
    "absolute.hdfs.path": "/etl/toy_store/products/1488467463898",
    "metadata.table.targetMergeStrategy": "DEDUPE_AND_MERGE",
    "merge.count": "1"
}
```

### Adding a New Hive Table
To add a new hive table to be cleaned, it must follow the same partitioning pattern seen in \_feed, \_invalid, \_valid tables.
The following steps are required
1. Add the table within Cleanup:setTableList()
2. Build the code

### Adding a New Directory (HDFS/Local)
To add a new directory (HDFS/Local), the directory must follow the same partitioning pattern seen in spark.input_folder.
The following steps are required
1. Add the directory within Cleanup:setDirectoryList
2. Designate if the directory is HDFS or Local
3. Build the code

## Changing the Rentention period
The Cleanup process uses **cleanup.retention.period** Nifi attribute to determine what partitions to delete. By default, this value is 7 days.
To change this, just add/modify this Nifi attribute before calling the Cleanup spark processor

### Adding Spark Job to standard-ingest
To add the Cleanup Job, the following changes need to be done to the standard-ingest template. These steps will done After the **Commit Highwater Mark** processor.
1. Add a **UpdateAttribute** processor which will provided configurable values for cleanup. In particular, the **cleanup.retention.period** attribute.
2. Add a **AttributesToJSON** processor which will convert the attributes to the JSON.
    1. Set the _Destination_ to flowfile-attribute
3. Add a **ExecuteScript** processor which will output the JSON attribute to a local file.
    1. Set the _Script Engine_ to Groovy
    2. Add the following script to the _Script Body_
```
    def flowFile = session.get()
      if(!flowFile) return
      def json = flowFile.getAttribute("JSONAttributes");
      def inputFolder = flowFile.getAttribute("spark.input_folder")
      def feed = flowFile.getAttribute("feed")
      def category = flowFile.getAttribute("category")
      def feedts = flowFile.getAttribute("feedts")
      def folder = new File(inputFolder + "/"+category+"/"+feed+"/"+feedts+"/"+"cleanup")
      // If it doesn't exist
      if( !folder.exists() ) {
      // Create all folders
      folder.mkdirs()
      }
      def jsonFile = new File(folder,feed+"_attributes.json")
      jsonFile.write(json)
      flowFile = session.putAttribute(flowFile,"attribute.json.path",jsonFile.getCanonicalPath())
      session.transfer(flowFile, REL_SUCCESS)
```
4. Add a **ExecuteSparkJob** processor which will execute the Spark job
    * Populate all the necessary Spark attributes
        * __ApplicationJar__ = /path/to/jar/kylo-spark-job-cleanup-spark-v1-<version>-jar-with-dependencies.jar
        * __MainClass__ =  com.thinkbiganalytics.spark.cleanup.Cleanup
        * __MainArgs__ to ${attribute.json.path}
