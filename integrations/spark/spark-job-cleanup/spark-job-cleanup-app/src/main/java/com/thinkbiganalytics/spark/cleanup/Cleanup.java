package com.thinkbiganalytics.spark.cleanup;

/*-
 * #%L
 * kylo-spark-job-cleanup-app
 * %%
 * Copyright (C) 2017 ThinkBig Analytics
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.thinkbiganalytics.spark.SparkContextService;
import com.thinkbiganalytics.spark.cleanup.util.CleanupConstants;
import com.thinkbiganalytics.spark.cleanup.util.FlowAttributes;
import com.thinkbiganalytics.spark.cleanup.util.MissingAttributeException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class performs a cleanup of staging/working files used by the standard-ingest template.
 * Currently, there are three major areas that are touched:<br>
 * 1. Hive tables (_feed, _valid, _invalid, _profile, _dataquality)<br>
 * 2. HDFS directories (partition_dttm)<br>
 * 3. Local file directories (partitioning_dttm)<br>
 * <br>
 * This class takes in one argument which is the path of the JSON file which contains all the
 * flowfile attributes<br>
 * <br>
 * If this class fails, it is NOT passed back to the processor i.e. failures are okay. The
 * assumption is that this will be executed for each feed for each job and a single run will cleanup
 * the previous failures. <br>
 * Please refer to README for commands to run application.<br>
 */
@Component
public class Cleanup {

    private static final Logger log = LoggerFactory.getLogger(Cleanup.class);

    @Autowired
    private SparkContextService scs;
    private HiveContext hiveContext;

    private FlowAttributes flowAttributes;
    private List<Long> partitionDeleteList;
    private List<String> tableList;
    // Mapped as <local/hdfs, full path>
    private Map<String, String> directoryList;
    
    private List<String> summaryList;

    public static void main(String[] args) {
        log.info("Running Cleanup with these command line args: " + StringUtils.join(args, ","));

        if (args.length < 1) {
            System.out.println("Expected command line args: <path-to-attribute-file>");
            System.exit(1);
        }

        try {
            ApplicationContext ctx = new AnnotationConfigApplicationContext("com.thinkbiganalytics.spark");
            Cleanup app = ctx.getBean(Cleanup.class);

            app.setArguments(args[0]);

            boolean success = app.doCleanup();
            
            if (success) {
                log.info("Cleanup was successful");
            }
            else {
                log.error("Cleanup failed. Check log for details");
                System.exit(1);
            }
        } catch (Exception e) {
            log.error("Failed to perform cleanup: {}", e.getMessage());
            System.exit(1);
        }

        log.info("Cleanup has finished.");
        System.exit(0);
    }

    public Cleanup() {
        flowAttributes = new FlowAttributes();
        partitionDeleteList = new ArrayList<>();
        tableList = new ArrayList<>();
        directoryList = new HashMap<>();
        summaryList = new ArrayList<>();
    }

    public boolean doCleanup() {
        boolean pass = false;
        
        try {
            
            if (flowAttributes.isEmpty()) {
                log.error("No Flow Attributes due to bad input/argument file");
                return pass;
            }
            
            SparkContext sparkContext = SparkContext.getOrCreate();
            hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkContext);

            String category = flowAttributes.getAttributeValue(CleanupConstants.CATEGORY_ATTRIBUTE);
            String feed = flowAttributes.getAttributeValue(CleanupConstants.FEED_ATTRIBUTE);
            String feedts = flowAttributes.getAttributeValue(CleanupConstants.PROCESSING_DTTM_ATTRIBUTE);

            String hdfsRetention = flowAttributes.getAttributeValue(CleanupConstants.CLEANUP_RETENTION_ATTRIBUTE,
                                                                    CleanupConstants.DEFAULT_CLEANUP_RETENTION_VALUE);

            long lowWatermark = calculateLowWatermark(Long.parseLong(feedts),
                                                      Long.parseLong(hdfsRetention));

            setTableList(category, feed);

            // Assumption is first table is the _feed table
            List<Long> existingPartitionList = getTablePartitions(tableList.get(0));

            setDeleteList(existingPartitionList, lowWatermark);

            cleanupTables();

            setDirectoryList();

            cleanupDirectories();
            
            outputSummary();
            
            pass = true;
            

        } catch (MissingAttributeException e) {
            log.error("Required Attribute missing from passed in data", e);
        } catch (Exception e) {
            log.error("Unknown exception while doCleanup()", e);
        }
        
        return pass;
    }



    /**
     * Sets the names of the tables that will be cleaned. Tables will be prefixed with the database
     * name
     * 
     * @param databaseName
     * @param tableName
     */
    protected void setTableList(String databaseName, String tableName) {
        tableList.add(databaseName + "." + tableName + CleanupConstants.FEED_TABLE_SUFFIX);
        tableList.add(databaseName + "." + tableName + CleanupConstants.INVALID_TABLE_SUFFIX);
        tableList.add(databaseName + "." + tableName + CleanupConstants.VALID_TABLE_SUFFIX);
        tableList.add(databaseName + "." + tableName + CleanupConstants.PROFILE_TABLE_SUFFIX);
        tableList.add(databaseName + "." + tableName + CleanupConstants.DQ_TABLE_SUFFIX);
    }

    /**
     * Creates the list of directories that will be assessed for deletion
     * 
     */
    private void setDirectoryList() {

        try {
            String category = flowAttributes.getAttributeValue(CleanupConstants.CATEGORY_ATTRIBUTE);
            String feed = flowAttributes.getAttributeValue(CleanupConstants.FEED_ATTRIBUTE);
            String failedRoot = flowAttributes.getAttributeValue(CleanupConstants.FAILED_ROOT_ATTRIBUTE,
                                                                 CleanupConstants.DEFAULT_FAILED_ROOT_VALUE);
            String hdfsIngestRoot = flowAttributes.getAttributeValue(CleanupConstants.HDFS_INGEST_ROOT_ATTRIBUTE,
                                                                     CleanupConstants.DEFAULT_INGEST_ROOT_VALUE);
            String sparkInputFolder = flowAttributes.getAttributeValue(CleanupConstants.SPARK_INPUT_FOLDER_ATTRIBUTE,
                                                                       CleanupConstants.DEFAULT_SPARK_FOLDER_VALUE);

            String failedDir = failedRoot + "/" + category + "/" + feed;
            String hdfsIngestDir = hdfsIngestRoot + "/" + category + "/" + feed;
            String localSparkDir = sparkInputFolder + "/" + category + "/" + feed;

            directoryList.put(CleanupConstants.LOCAL_DIRECTORY, failedDir);
            directoryList.put(CleanupConstants.LOCAL_DIRECTORY, hdfsIngestDir);
            directoryList.put(CleanupConstants.LOCAL_DIRECTORY, localSparkDir);

        } catch (MissingAttributeException e) {
            log.error("Required Attribute missing from passed in data", e);
        }


    }

    /**
     * Calculates the low watermark which is used as the cutoff value. Anything older than the low
     * watermark will be deleted
     * 
     * @param currentTime Current time in seconds
     * @param retentionVal Retention in days
     * @return Long value representing the low watermark
     */
    protected long calculateLowWatermark(long currentTime, long retentionVal) {
        long retentionSecs = retentionVal * CleanupConstants.DAYS_TO_MILLISECONDS;

        long lowWatermark = currentTime - retentionSecs;

        // If the low watermark is negative, return 0
        if (lowWatermark < 0) {
            lowWatermark = 0L;
        }

        String msg = "Low Watermark set to " + lowWatermark;
        log.info(msg);
        summaryList.add(msg);

        return lowWatermark;

    }

    /**
     * Takes the passed in hive table and returns a list of partitions. This assumes that the table
     * is partitioned by processing_dttm
     * 
     * @param tableName Name of the hive table with database prefix
     * @return List of partitions
     */
    private List<Long> getTablePartitions(String tableName) {

        List<Long> partitionsList = new ArrayList<Long>();

        try {

            String query = "SHOW PARTITIONS " + tableName;

            log.info("Executing hive query: " + query);

            DataFrame resultDF = hiveContext.sql(query);
            List<Row> rowList = resultDF.collectAsList();

            // Convert row values to long
            for (Row row : rowList) {
                String val = row.getString(0).split("=")[1];
                partitionsList.add(Long.parseLong(val));
            }
            
            log.info("Number of partitions = " + partitionsList.size());

        } catch (NumberFormatException e) {
            log.error("Error while converting string partition value to long", e);

        } catch (Exception e) {
            log.error("ERROR - Error while getting partition List. Parameters were " +
                      " table = "
                      + tableName, e);
        }

        return partitionsList;
    }

    /**
     * Determines which partitions needs to be deleted.<br>
     * This is done by returning partitions values that are less than the low watermark value
     * 
     * @param existingPartitionList All the partitions for the feed
     * @param lowWatermark Cutoff value for deletion (partitions less than this will be removed)
     */
    private void setDeleteList(List<Long> existingPartitionList, final long lowWatermark) {

        for (Long partition : existingPartitionList) {
            if (partition <= lowWatermark) {
                partitionDeleteList.add(partition);
            }
        }

        String msg = "Number of partitions to delete = " + partitionDeleteList.size();
        log.info(msg);
        summaryList.add(msg);
    }

    /**
     * Iterates through tableList and deletes partitions marked for deletion
     */
    protected void cleanupTables() {
        for (String tableName : tableList) {
            dropTablePartitions(tableName);
        }
    }

    /**
     * Uses the passed in arguments to delete partitions from the Hive tables
     * 
     * @param tableName Table to delete the partitions from
     */
    private boolean dropTablePartitions(String tableName) {
        boolean status = true;

        try {
            for (long partition : partitionDeleteList) {
                String dropQuery = "ALTER TABLE " + tableName
                                   + " DROP IF EXISTS PARTITION (processing_dttm="
                                   +
                                   Long.toString(partition)
                                   + ")";

                log.info("Executing query: " + dropQuery);

                hiveContext.sql(dropQuery);
                
                summaryList.add("For table: " + tableName + ", Dropped partition = " + partition);
            }

        } catch (Exception e) {
            log.error("ERROR - Error while deleting partitions. Parameters were " +
                      " table = "
                      + tableName, e);

            return false;
        }

        return status;
    }

    /**
     * Iterates through the directoryList and deletes partition directories marked for deletion
     * 
     */
    protected void cleanupDirectories() {
        for (Map.Entry<String, String> entry : directoryList.entrySet()) {
            if (entry.getKey().equals(CleanupConstants.LOCAL_DIRECTORY)) {
                dropLocalDirectory(entry.getValue());
            } else {
                dropHDFSDirectory(entry.getValue());
            }
        }
    }

    /**
     * Uses the passed in arguments to delete HDFS directories based on the feed partitions found in
     * deletePartitionList
     * 
     * @param hdfsBaseDir HDFS base directory that contains data to be deleted
     */
    protected boolean dropHDFSDirectory(String hdfsBaseDir) {
        try {

            for (long partition : partitionDeleteList) {
                String hdfsDir = hdfsBaseDir + "/" + Long.toString(partition);

                URI uri = URI.create(hdfsDir);
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(uri, conf);
                Path hdfsPath = new Path(hdfsDir);

                log.info("Deleting HDFS dir: " + hdfsPath);
                fs.delete(hdfsPath, true);
                
                summaryList.add("Deleted HDFS Directory: " + hdfsPath);
            }

        } catch (IOException e) {
            log.error("ERROR - Error when deleting HDFS directory. Parameters were"
                      + "HDFS base directory = " + hdfsBaseDir, e);

            return false;
        }

        return true;
    }

    /**
     * Uses passed in arguments to delete local files based on the partitions within the
     * deletePartitionList
     * 
     * @param localBaseDir Local base directory that contains data to be deleted
     */
    protected boolean dropLocalDirectory(String localBaseDir) {
        try {

            for (long partition : partitionDeleteList) {
                String localDir = localBaseDir + "/" + Long.toString(partition);

                log.info("Deleting local dir: " + localDir);
                FileUtils.deleteDirectory(new File(localDir));
                
                summaryList.add("Deleted Local Directory: " + localDir);
            }

        } catch (Exception e) {
            log.error("ERROR - Error when deleting Local directory. Parameters were"
                      + "HDFS base directory = " + localBaseDir, e);

            return false;
        }

        return true;

    }
    
    protected void outputSummary() {
 
        StringBuffer summary = new StringBuffer();
        for (String s: summaryList) {
            summary.append(s + "\n");
        }
        
        log.info("Cleanup summary: " + summary.toString());
    }

    protected HiveContext getHiveContext() {
        return hiveContext;
    }

    public FlowAttributes getFlowAttributes() {
        return flowAttributes;
    }

    public List<Long> getPartitionDeleteList() {
        return partitionDeleteList;
    }

    public List<String> getTableList() {
        return tableList;
    }

    public Map<String, String> getDirectoryList() {
        return directoryList;
    }

    /**
     * Uses the passed JSON path to set the arguments
     * 
     * @param attributesJsonPath Path to the JSON file
     */
    public void setArguments(String attributesJsonPath) {
        flowAttributes.setAttributes(attributesJsonPath);
    }

    public void setFlowAttributes(FlowAttributes flowAttributes) {
        this.flowAttributes = flowAttributes;
    }

    public void setPartitionDeleteList(List<Long> partitionDeleteList) {
        this.partitionDeleteList = partitionDeleteList;
    }

    public void setTableList(List<String> tableList) {
        this.tableList = tableList;
    }

    public void setDirectoryList(Map<String, String> directoryList) {
        this.directoryList = directoryList;
    }
}
