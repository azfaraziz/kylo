package com.thinkbiganalytics.spark.dataquality.checker;

/*-
 * #%L
 * kylo-spark-job-dataquality-app
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

import com.thinkbiganalytics.spark.dataquality.util.FlowAttributes;
import com.thinkbiganalytics.spark.dataquality.util.MissingAttributeException;
import com.thinkbiganalytics.spark.dataquality.util.MissingRuleException;
import com.thinkbiganalytics.spark.dataquality.util.DataQualityConstants;
import com.thinkbiganalytics.spark.dataquality.output.DataQualityRow;
import com.thinkbiganalytics.spark.dataquality.output.DataQualityWriter;

import com.thinkbiganalytics.spark.dataquality.rule.DataQualityRule;
import com.thinkbiganalytics.spark.dataquality.rule.InvalidRowPercentRuleImpl;
import com.thinkbiganalytics.spark.dataquality.rule.InvalidRowTotalCountRuleImpl;
import com.thinkbiganalytics.spark.dataquality.rule.RowCountRuleImpl;
import com.thinkbiganalytics.spark.dataquality.rule.SourceToFeedCountRuleImpl;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class performs the Data Quality Checks. This is done by executing various Data Quality rules
 * and using the passed in JSON file which contains all the necessary attributes. The results of all
 * the tests are stored into a feed_dataquality hive table<br>
 * <br>
 * This class takes in one argument which is the path of the JSON file which contains all the
 * flowfile attributes<br>
 * <br>
 * Failure is considered to be if any of the rules failed. If at least one of the rules fails, the 
 * overall execution is considered a failure. The execution is an all or nothing process.<br>
 * <br>
 * Please refer to README for commands to run application.<br>
 */
@Component
public class DataQualityChecker {
    private static final Logger log = LoggerFactory.getLogger(DataQualityChecker.class);

    @Autowired
    private SparkContextService scs;
    private HiveContext hiveContext;

    private FlowAttributes flowAttributes;
    private Map<String, DataQualityRule> availableRules;
    private List<DataQualityRule> ruleList;

    public static void main(String[] args) {
        log.info("Running DataQualityChecker with these command line args: {}", StringUtils.join(args, ","));

        if (args.length < 1) {
            System.out.println("Expected command line args: <path-to-attribute-file>");
            System.exit(1);
        }

        try {
            ApplicationContext ctx = new AnnotationConfigApplicationContext("com.thinkbiganalytics.spark");
            DataQualityChecker app = ctx.getBean(DataQualityChecker.class);

            app.setArguments(args[0]);
            app.setAvailableRules();
            app.setActiveRules();

            boolean isSuccess = app.doDataQualityChecks();
            if (isSuccess) {
                log.info("DataQualityChecker has PASSED.");
            } else {
                log.info("DataQualityChecker has FAILED.");
                System.exit(1);
            }

        } catch (MissingRuleException e) {
            log.error("One or more rules are not available: {}", e);
            System.exit(1);
        } catch (Exception e) {
            log.error("Failed to perform data quality checks: {}", e);
            System.exit(1);
        }
    }

    public DataQualityChecker() {
        flowAttributes = new FlowAttributes();
        ruleList = new ArrayList<>();
        availableRules = new HashMap<>();
    }

    /**
     * Adds all the data quality rules used by the checker
     */
    protected void setAvailableRules() {
        addAvailableRule(new SourceToFeedCountRuleImpl());
        addAvailableRule(new RowCountRuleImpl());
        addAvailableRule(new InvalidRowTotalCountRuleImpl());
        addAvailableRule(new InvalidRowPercentRuleImpl());
    }

    /**
     * Sets the active rules based on the available rule list. The flow attribute dq.active.rules is
     * used to specify active rules. If this attribute does not exist, no rules are used.<br>
     * If any rule does not exist, an exception is thrown
     * 
     * @throws MissingRuleException Exception thrown when the Data Quality rule is missing
     */
    protected void setActiveRules() throws MissingRuleException {

        String activeRuleStr = flowAttributes.getAttributeValue(DataQualityConstants.DQ_ACTIVE_RULES_ATTRIBUTE,
                                                                DataQualityConstants.DEFAULT_DQ_ACTIVE_RULES_VALUE);

        if (activeRuleStr.equals(DataQualityConstants.DEFAULT_DQ_ACTIVE_RULES_VALUE)) {
            log.info("No rules are set. No rules will be used");
            ruleList.clear();
        } else if (activeRuleStr.equals(DataQualityConstants.ALL_DQ_ACTIVE_RULES_VALUES)) {
            log.info("All available Data Quality Rules will be used");
            ruleList = new ArrayList<>(availableRules.values());
        } else {
            String[] parsedRuleStr = activeRuleStr.split(",");

            for (String str : parsedRuleStr) {
                addDataQualityRule(getAvailableRule(str.trim()));
            }
        }
    }

    /**
     * Main method that conducts the data quality check. This is done by getting row counts and
     * iterate through each data quality check. The results summary is provided after all are
     * executed. The results are written to log as well as hive
     * 
     * @return Boolean value is the checks were successful
     */
    protected boolean doDataQualityChecks() {

        boolean isSuccessful = false;
        try {
            String databaseName = flowAttributes.getAttributeValue(DataQualityConstants.CATEGORY_ATTRIBUTE);
            String tableName = flowAttributes.getAttributeValue(DataQualityConstants.FEED_ATTRIBUTE);
            String processingDttm = flowAttributes.getAttributeValue(DataQualityConstants.PROCESSING_DTTM_ATTRIBUTE);
            
            if (processingDttm.isEmpty()) {
                String msg = "Required attribute processing_dttm is empty. Failing DQ";
                log.error(msg);
                throw new MissingAttributeException(msg);
            }

            String feedTableName = tableName + DataQualityConstants.FEED_TABLE_SUFFIX;
            String invalidTableName = tableName + DataQualityConstants.INVALID_TABLE_SUFFIX;
            String validTableName = tableName + DataQualityConstants.VALID_TABLE_SUFFIX;
            String whereClause = DataQualityConstants.PROCESSING_DTTM_COLUMN + " = '" + processingDttm + "'";

            setSourceRowCount();

            long rowCount;
            rowCount = getRowCount(databaseName, feedTableName, whereClause);
            flowAttributes.addAttribute(DataQualityConstants.DQ_FEED_ROW_COUNT_ATTRIBUTE, String.valueOf(rowCount));

            rowCount = getRowCount(databaseName, invalidTableName, whereClause);
            flowAttributes.addAttribute(DataQualityConstants.DQ_INVALID_ROW_COUNT_ATTRIBUTE, String.valueOf(rowCount));

            rowCount = getRowCount(databaseName, validTableName, whereClause);
            flowAttributes.addAttribute(DataQualityConstants.DQ_VALID_ROW_COUNT_ATTRIBUTE, String.valueOf(rowCount));

            // Execute Data Quality rules
            boolean rulePass = true;
            boolean dqAllRulePass = true;
            for (DataQualityRule rule : ruleList) {

                log.info("Executing rule: " + rule.getName());

                if (rule.loadAttributes(flowAttributes)) {
                    rulePass = rule.evaluate();
                } else {
                    rulePass = false;
                }

                if (!rulePass) {
                    log.error("Rule: " + rule.getName() + " has FAILED. Rule desc: " + rule.getDescription());
                } else {
                    log.info("Rule: " + rule.getName() + " has PASSED");
                }

                dqAllRulePass = dqAllRulePass && rulePass;

            }

            outputToHive();

            outputToLog();

            // Return false if any rule failed
            if (dqAllRulePass == false) {
                log.warn("Data Quality check failures");
                isSuccessful = false;
            } else {
                isSuccessful = true;
            }

        } catch (MissingAttributeException e) {
            log.error("Required Attribute missing from passed in data", e);
            isSuccessful = false;
        } catch (Exception e) {
            log.error("Generic exception in doQualityCheck()", e);
            isSuccessful = false;
        }

        return isSuccessful;
    }

    /**
     * This method will set the source row count if it does not already exists. This happens when
     * other processors are used to pull data, such as ImportSqoop
     * 
     * If ImportSqoop Nifi processor is used, it updates the source.row.count value with the sqoop
     * count
     */
    protected void setSourceRowCount() {
        try {
            if (flowAttributes.containsAttribute(DataQualityConstants.SQOOP_ROW_COUNT_ATTRIBUTE)) {
                String sqoopRowCount = flowAttributes.getAttributeValue(DataQualityConstants.SQOOP_ROW_COUNT_ATTRIBUTE);
                flowAttributes.addAttribute(DataQualityConstants.SOURCE_ROW_COUNT_ATTRIBUTE, sqoopRowCount);
            }
        } catch (MissingAttributeException e) {
            log.error("Required Attribute missing", e);
        }
    }

    /**
     * Given the passed in arguments, calculates the row counts
     * 
     * @param databaseName Name of the Hive database
     * @param tableName Name of the Hive table
     * @param whereClause Additional filters
     * @return row count for the table
     */
    protected long getRowCount(String databaseName, String tableName, String whereClause) {
        try {

            SparkContext sparkContext = SparkContext.getOrCreate();
            hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkContext);

            String query = "SELECT COUNT(*) FROM " + databaseName
                           + "."
                           + tableName
                           + " ";

            if (!whereClause.equals("")) {
                query = query + "WHERE " + whereClause;
            }

            log.info("Executing hive query: %s", query);

            DataFrame countDF = hiveContext.sql(query);

            // Only take the first value which contains the row count
            return countDF.collect()[0].getLong(0);

        } catch (Exception e) {
            String errMessage = "ERROR - Error while getting row count. Parameters were " +
                            " database = "
                            + databaseName
                            +
                            " table = "
                            + tableName
                            +
                            " whereClause = "
                            + whereClause;
            log.error(errMessage, e);

            throw e;
        }
    }

    /**
     * Write the results to a Hive table
     */
    private void outputToHive() {

        try {
            String databaseName = flowAttributes.getAttributeValue(DataQualityConstants.CATEGORY_ATTRIBUTE);
            String tableName = flowAttributes.getAttributeValue(DataQualityConstants.FEED_ATTRIBUTE);
            String dqTableName = tableName + DataQualityConstants.DQ_TABLE_SUFFIX;
            String processingDttm = flowAttributes.getAttributeValue(DataQualityConstants.PROCESSING_DTTM_ATTRIBUTE);

            SparkContext sparkContext = SparkContext.getOrCreate();
            hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkContext);

            DataQualityWriter dqWriter = DataQualityWriter.getInstance();
            dqWriter.setHiveContext(hiveContext);
            dqWriter.setSparkContext(new JavaSparkContext(sparkContext));

            for (DataQualityRule rule : ruleList) {
                dqWriter.addRow(new DataQualityRow(rule));
            }

            dqWriter.writeResultToTable(scs,
                                        databaseName + "." + dqTableName,
                                        processingDttm);

        } catch (MissingAttributeException e) {
            log.error("Required Attribute missing from passed in data", e);
        } catch (Exception e) {
            log.error("Generic exception in outputToHive()", e);
        }


    }

    /**
     * Write the results to the log
     */
    @SuppressWarnings("unchecked")
    private void outputToLog() {

        JSONObject summaryJSON = new JSONObject();
        for (DataQualityRule rule : ruleList) {
            summaryJSON.put(rule.getName(), rule.getSummary());
        }
        log.info("Data Quality Summary - {}", summaryJSON.toJSONString());

    }

    /**
     * Uses the passed JSON path to set the arguments
     * 
     * @param attributesJsonPath Path to the JSON file
     */
    public void setArguments(String attributesJsonPath) {
        flowAttributes.setAttributes(attributesJsonPath);
    }

    /**
     * Add a Data Quality rule to the rule list
     * 
     * @param rule Data Quality rule to add
     */
    protected void addDataQualityRule(DataQualityRule rule) {
        ruleList.add(rule);
    }

    /**
     * Add to the available Data Quality rules
     * 
     * @param rule Data Quality rule to add
     */
    protected void addAvailableRule(DataQualityRule rule) {
        availableRules.put(rule.getName(), rule);
    }

    /**
     * Gets the DataQualityRule object based on the passed in rule name. If the rule does not exist,
     * an exception is thrown
     * 
     * @param ruleName Name of the rule
     * @return DataQualityRule object
     * @throws MissingRuleException Exception thrown when rule is not part of the available set
     */
    protected DataQualityRule getAvailableRule(String ruleName) throws MissingRuleException {
        DataQualityRule rule = availableRules.get(ruleName);

        if (rule == null) {
            String msg = "Rule: " + ruleName + " is not an available rule";
            throw new MissingRuleException(msg);
        }

        return rule;
    }

    protected FlowAttributes getAttributes() {
        return flowAttributes;
    }

    protected void setAttributes(FlowAttributes flowAttr) {
        flowAttributes = flowAttr;
    }

    protected HiveContext getHiveContext() {
        return hiveContext;
    }

    public List<DataQualityRule> getRuleList() {
        return ruleList;
    }

    public void setRuleList(List<DataQualityRule> ruleList) {
        this.ruleList = ruleList;
    }

    public Map<String, DataQualityRule> getAvailableRules() {
        return availableRules;
    }

    public void setAvailableRules(Map<String, DataQualityRule> availableRules) {
        this.availableRules = availableRules;
    }
}
