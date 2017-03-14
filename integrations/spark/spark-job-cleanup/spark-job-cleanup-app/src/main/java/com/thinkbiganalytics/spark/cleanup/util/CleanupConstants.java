package com.thinkbiganalytics.spark.cleanup.util;

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

/**
 * Constants used for the Clean up process
 */
public class CleanupConstants {


    /**
     * Constants
     */
    public static final long DAYS_TO_SECONDS = 86400;
    public static final long DAYS_TO_MILLISECONDS = 86400000;
    public static final String LOCAL_DIRECTORY = "LOCAL";
    public static final String HDFS_DIRECTORY = "HDFS";
    public static final String PROCESSING_DTTM_COLUMN = "processing_dttm";
    public static final String VALID_TABLE_SUFFIX = "_valid";
    public static final String INVALID_TABLE_SUFFIX = "_invalid";
    public static final String FEED_TABLE_SUFFIX = "_feed";
    public static final String PROFILE_TABLE_SUFFIX = "_profile";
    public static final String DQ_TABLE_SUFFIX = "_dataquality";

    /**
     * Nifi Attributes coming from FlowFile
     */
    public static final String CATEGORY_ATTRIBUTE = "category";
    public static final String FEED_ATTRIBUTE = "feed";
    public static final String PROCESSING_DTTM_ATTRIBUTE = "feedts";
    public static final String FAILED_ROOT_ATTRIBUTE = "local.failed.root";
    public static final String HDFS_FAILED_ROOT_ATTRIBUTE = "hdfs.failed.root";
    public static final String HDFS_INGEST_ROOT_ATTRIBUTE = "hdfs.ingest.root";
    public static final String SPARK_INPUT_FOLDER_ATTRIBUTE = "spark.input_folder";
    public static final String CLEANUP_RETENTION_ATTRIBUTE = "cleanup.retention.period";

    /**
     * Default Values used by Cleanup code
     */
    public static final String DEFAULT_CLEANUP_RETENTION_VALUE = "7"; // Days
    public static final String DEFAULT_FAILED_ROOT_VALUE = "/tmp/kylo-nifi";
    public static final String DEFAULT_INGEST_ROOT_VALUE = "/etl";
    public static final String DEFAULT_SPARK_FOLDER_VALUE = "/tmp/kylo-nifi/spark";


}
