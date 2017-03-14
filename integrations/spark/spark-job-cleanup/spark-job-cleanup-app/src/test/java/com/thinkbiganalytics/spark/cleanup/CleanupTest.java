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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.thinkbiganalytics.spark.cleanup.util.CleanupConstants;
import com.thinkbiganalytics.spark.cleanup.util.FlowAttributes;
import com.thinkbiganalytics.spark.cleanup.util.MissingAttributeException;

public class CleanupTest {

    private final String RESOURCE_LOCATION = "src/test/resources/";
    private final String ATTRIBUTE_JSON_PATH = RESOURCE_LOCATION + "sample.json";

    Cleanup cleanup = new Cleanup();

    @Before
    public void setUp() {
        cleanup.setArguments(ATTRIBUTE_JSON_PATH);
    }

    @Test
    public void testSetArguments() {

        String expectedVal = "toy_store";
        int expectedAttrCount = 38;

        try {

            String actualVal = cleanup.getFlowAttributes().getAttributeValue(CleanupConstants.CATEGORY_ATTRIBUTE);
            int actualAttrCount = cleanup.getFlowAttributes().count();

            assertTrue("Attributes for " + CleanupConstants.CATEGORY_ATTRIBUTE
                       + " do not match"
                       + "Expected = "
                       + expectedVal
                       + " Actual = "
                       + actualVal,
                       expectedVal.equals(actualVal));

            assertTrue("Attribute counts do not match. "
                       + "Expected = "
                       + expectedAttrCount
                       + " Actual = "
                       + actualAttrCount,
                       expectedAttrCount == actualAttrCount);

        } catch (MissingAttributeException e) {
            e.printStackTrace();
            assert (false);
        }
    }

    @Test
    public void testLowWatermark() {

        long expectedVal = Long.parseUnsignedLong("1487862663898");

        FlowAttributes flowAttr = cleanup.getFlowAttributes();

        try {

            String feedts = flowAttr.getAttributeValue(CleanupConstants.PROCESSING_DTTM_ATTRIBUTE);
            String hdfsRetention = flowAttr.getAttributeValue(CleanupConstants.CLEANUP_RETENTION_ATTRIBUTE,
                                                              CleanupConstants.DEFAULT_CLEANUP_RETENTION_VALUE);

            long actualVal = cleanup.calculateLowWatermark(Long.parseUnsignedLong(feedts),
                                                           Long.parseUnsignedLong(hdfsRetention));

            assertTrue("Low watermark do not match: expected= " + expectedVal + " actual= " + actualVal,
                       expectedVal == actualVal);

        } catch (MissingAttributeException e) {
            e.printStackTrace();
            assert (false);
        }
    }

    @Test
    public void testDropLocalDirectory() throws IOException, MissingAttributeException {

        FlowAttributes flowAttr = cleanup.getFlowAttributes();
        String category = flowAttr.getAttributeValue(CleanupConstants.CATEGORY_ATTRIBUTE);
        String feed = flowAttr.getAttributeValue(CleanupConstants.FEED_ATTRIBUTE);
        String feedts = flowAttr.getAttributeValue(CleanupConstants.PROCESSING_DTTM_ATTRIBUTE);

        List<Long> partitionList = new ArrayList<>();
        partitionList.add(Long.valueOf(feedts));
        cleanup.setPartitionDeleteList(partitionList);


        String baseDir = FileUtils.getTempDirectoryPath() + "/" + category + "/" + feed;
        String partitionDir = baseDir + "/" + feedts;
        File partitionFile = new File(partitionDir);

        // Create temporary partition
        FileUtils.forceMkdir(partitionFile);

        cleanup.dropLocalDirectory(baseDir);

        assertTrue("Local file was not deleted", !partitionFile.exists());
    }

}
