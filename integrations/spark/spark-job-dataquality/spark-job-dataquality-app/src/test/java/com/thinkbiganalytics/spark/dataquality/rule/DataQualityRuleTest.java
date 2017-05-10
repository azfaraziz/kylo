package com.thinkbiganalytics.spark.dataquality.rule;


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

import org.junit.Before;

import com.thinkbiganalytics.spark.dataquality.util.DataQualityConstants;
import com.thinkbiganalytics.spark.dataquality.util.FlowAttributes;

public abstract class DataQualityRuleTest {

    FlowAttributes flowAttributes = new FlowAttributes();

    @Before
    public void setUp() {
        flowAttributes.addAttribute(DataQualityConstants.DQ_FEED_ROW_COUNT_ATTRIBUTE, "100");
        flowAttributes.addAttribute(DataQualityConstants.SOURCE_ROW_COUNT_ATTRIBUTE, "100");

        flowAttributes.addAttribute(DataQualityConstants.DQ_VALID_ROW_COUNT_ATTRIBUTE, "100");
        flowAttributes.addAttribute(DataQualityConstants.DQ_INVALID_ROW_COUNT_ATTRIBUTE, "0");

        flowAttributes.addAttribute(DataQualityConstants.DQ_INVALID_ALLOWED_COUNT_ATTRIBUTE, "0");
    }

    /**
     * Generic method to run rules and print summary
     * 
     * @param rule Rule that will be executed
     * @return pass/fail of rule
     */
    public boolean runRule(DataQualityRule rule) {
        rule.loadAttributes(flowAttributes);
        
        boolean result = rule.evaluate();
        System.out.println(rule.getSummary());

        return result;
    }
}