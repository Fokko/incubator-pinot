/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core.realtime.segment;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.core.realtime.stream.PartitionLevelStreamConfig;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.apache.pinot.core.realtime.stream.StreamConfigProperties;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class FlushThresholdUpdaterTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final long DESIRED_SEGMENT_SIZE = StreamConfig.getDefaultDesiredSegmentSizeBytes();
  private static final int DEFAULT_INITIAL_ROWS_THRESHOLD = StreamConfig.getDefaultFlushAutotuneInitialRows();

  private Random _random;
  private Map<String, double[][]> datasetGraph;

  @BeforeClass
  public void setup() {
    long seed = new Random().nextLong();
    System.out.println("Random seed for " + FlushThresholdUpdater.class.getSimpleName() + " is " + seed);
    _random = new Random(seed);

    datasetGraph = new HashMap<>(3);
    double[][] exponentialGrowth =
        {{100000, 50}, {200000, 60}, {300000, 70}, {400000, 83}, {500000, 98}, {600000, 120}, {700000, 160}, {800000, 200}, {900000, 250}, {1000000, 310}, {1100000, 400}, {1200000, 500}, {1300000, 600}, {1400000, 700}, {1500000, 800}, {1600000, 950}, {1700000, 1130}, {1800000, 1400}, {1900000, 1700}, {2000000, 2000}};
    double[][] logarithmicGrowth =
        {{100000, 70}, {200000, 180}, {300000, 290}, {400000, 400}, {500000, 500}, {600000, 605}, {700000, 690}, {800000, 770}, {900000, 820}, {1000000, 865}, {1100000, 895}, {1200000, 920}, {1300000, 940}, {1400000, 955}, {1500000, 970}, {1600000, 980}, {1700000, 1000}, {1800000, 1012}, {1900000, 1020}, {2000000, 1030}};
    double[][] steps =
        {{100000, 100}, {200000, 100}, {300000, 200}, {400000, 200}, {500000, 300}, {600000, 300}, {700000, 400}, {800000, 400}, {900000, 500}, {1000000, 500}, {1100000, 600}, {1200000, 600}, {1300000, 700}, {1400000, 700}, {1500000, 800}, {1600000, 800}, {1700000, 900}, {1800000, 900}, {1900000, 1000}, {20000000, 1000}};
    datasetGraph.put("exponentialGrowth", exponentialGrowth);
    datasetGraph.put("logarithmicGrowth", logarithmicGrowth);
    datasetGraph.put("steps", steps);
  }

  /**
   * Tests the segment size based flush threshold updater. A series of 500 runs is started.
   * We have 3 types of datasets, each having a different segment size to num rows ratio (exponential growth, logarithmic growth, steps)
   * We let 500 segments pass through our algorithm, each time feeding a segment size based on the graph.
   * Towards the end, we begin to see that the segment size and number of rows begins to stabilize around the 500M mark
   */
  @Test
  public void testSegmentSizeBasedFlushThreshold() {
    for (Map.Entry<String, double[][]> entry : datasetGraph.entrySet()) {
      SegmentSizeBasedFlushThresholdUpdater segmentSizeBasedFlushThresholdUpdater =
          new SegmentSizeBasedFlushThresholdUpdater(DESIRED_SEGMENT_SIZE, DEFAULT_INITIAL_ROWS_THRESHOLD);

      double[][] numRowsToSegmentSize = entry.getValue();

      int numRuns = 500;
      double checkRunsAfter = 400;
      long idealSegmentSize = segmentSizeBasedFlushThresholdUpdater.getDesiredSegmentSizeBytes();
      long segmentSizeSwivel = (long) (idealSegmentSize * 0.5);
      int numRowsLowerLimit = 0;
      int numRowsUpperLimit = 0;
      for (int i = 0; i < numRowsToSegmentSize.length; i++) {
        if (numRowsToSegmentSize[i][1] * 1024 * 1024 >= idealSegmentSize) {
          numRowsLowerLimit = (int) numRowsToSegmentSize[i - 2][0];
          numRowsUpperLimit = (int) numRowsToSegmentSize[i + 3][0];
          break;
        }
      }
      long startOffset = 0;
      int seqNum = 0;
      int partitionId = 0;
      long segmentSizeBytes = 0;
      CommittingSegmentDescriptor committingSegmentDescriptor;
      LLCRealtimeSegmentZKMetadata committingSegmentMetadata;
      LLCRealtimeSegmentZKMetadata newSegmentMetadata;

      newSegmentMetadata = getNextSegmentMetadata(partitionId, seqNum++, startOffset, System.currentTimeMillis());
      committingSegmentDescriptor = new CommittingSegmentDescriptor(null, startOffset, segmentSizeBytes);
      segmentSizeBasedFlushThresholdUpdater
          .updateFlushThreshold(newSegmentMetadata, committingSegmentDescriptor, null, 0);
      assertEquals(newSegmentMetadata.getSizeThresholdToFlushSegment(),
          segmentSizeBasedFlushThresholdUpdater.getAutotuneInitialRows());

      for (int run = 0; run < numRuns; run++) {
        committingSegmentMetadata = new LLCRealtimeSegmentZKMetadata(newSegmentMetadata.toZNRecord());

        // get a segment size from the graph
        segmentSizeBytes =
            getSegmentSize(committingSegmentMetadata.getSizeThresholdToFlushSegment(), numRowsToSegmentSize);

        startOffset += 1000; // if stopped on time, increment less than 1000
        updateCommittingSegmentMetadata(committingSegmentMetadata, startOffset,
            committingSegmentMetadata.getSizeThresholdToFlushSegment());
        newSegmentMetadata = getNextSegmentMetadata(partitionId, seqNum++, startOffset, System.currentTimeMillis());
        committingSegmentDescriptor =
            new CommittingSegmentDescriptor(committingSegmentMetadata.getSegmentName(), startOffset, segmentSizeBytes);
        segmentSizeBasedFlushThresholdUpdater
            .updateFlushThreshold(newSegmentMetadata, committingSegmentDescriptor, committingSegmentMetadata, 0);

        // Assert that segment size is in limits
        if (run > checkRunsAfter) {
          assertTrue(segmentSizeBytes < (idealSegmentSize + segmentSizeSwivel),
              "Segment size check failed for dataset " + entry.getKey());
          assertTrue(committingSegmentMetadata.getSizeThresholdToFlushSegment() > numRowsLowerLimit
                  && committingSegmentMetadata.getSizeThresholdToFlushSegment() < numRowsUpperLimit,
              "Num rows check failed for dataset " + entry.getKey());
        }
      }
    }
  }

  long getSegmentSize(int numRowsConsumed, double[][] numRowsToSegmentSize) {
    double segmentSize = 0;
    if (numRowsConsumed < numRowsToSegmentSize[0][0]) {
      segmentSize = numRowsConsumed / numRowsToSegmentSize[0][0] * numRowsToSegmentSize[0][1];
    } else if (numRowsConsumed >= numRowsToSegmentSize[numRowsToSegmentSize.length - 1][0]) {
      segmentSize = numRowsConsumed / numRowsToSegmentSize[numRowsToSegmentSize.length - 1][0] * numRowsToSegmentSize[
          numRowsToSegmentSize.length - 1][1];
    } else {
      for (int i = 1; i < numRowsToSegmentSize.length; i++) {
        if (numRowsConsumed < numRowsToSegmentSize[i][0]) {
          segmentSize = _random.nextDouble() * (numRowsToSegmentSize[i][1] - numRowsToSegmentSize[i - 1][1])
              + numRowsToSegmentSize[i - 1][1];
          break;
        }
      }
    }
    return (long) (segmentSize * 1024 * 1024);
  }

  private LLCRealtimeSegmentZKMetadata getNextSegmentMetadata(int partitionId, int sequenceNumber, long startOffset,
      long creationTimeMs) {
    LLCRealtimeSegmentZKMetadata segmentZKMetadata = new LLCRealtimeSegmentZKMetadata();
    segmentZKMetadata.setSegmentName(
        new LLCSegmentName(RAW_TABLE_NAME, partitionId, sequenceNumber, creationTimeMs).getSegmentName());
    segmentZKMetadata.setStartOffset(startOffset);
    segmentZKMetadata.setEndOffset(Long.MAX_VALUE);
    segmentZKMetadata.setCreationTime(creationTimeMs);
    segmentZKMetadata.setNumReplicas(3);
    segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    return segmentZKMetadata;
  }

  private void updateCommittingSegmentMetadata(LLCRealtimeSegmentZKMetadata committingSegmentMetadata, long endOffset,
      long numDocs) {
    committingSegmentMetadata.setEndOffset(endOffset);
    committingSegmentMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    committingSegmentMetadata.setStartTime(System.currentTimeMillis());
    committingSegmentMetadata.setEndTime(System.currentTimeMillis());
    committingSegmentMetadata.setTotalRawDocs(numDocs);
  }

  /**
   * Tests that the flush threshold manager returns the right updater given various scenarios of flush threshold setting in the table config
   */
  @Test
  public void testFlushThresholdUpdater() {
    FlushThresholdUpdateManager flushThresholdUpdateManager = new FlushThresholdUpdateManager();
    Map<String, String> streamConfigMap = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap();

    // flush size set
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "10000");
    FlushThresholdUpdater flushThresholdUpdater = flushThresholdUpdateManager
        .getFlushThresholdUpdater(new PartitionLevelStreamConfig(REALTIME_TABLE_NAME, streamConfigMap));
    assertTrue(flushThresholdUpdater instanceof DefaultFlushThresholdUpdater);
    assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(), 10000);

    // llc flush size set
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS);
    streamConfigMap
        .put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS + StreamConfigProperties.LLC_SUFFIX, "5000");
    flushThresholdUpdater = flushThresholdUpdateManager
        .getFlushThresholdUpdater(new PartitionLevelStreamConfig(REALTIME_TABLE_NAME, streamConfigMap));
    assertTrue(flushThresholdUpdater instanceof DefaultFlushThresholdUpdater);
    assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(), 5000);

    // 0 flush size set
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS + StreamConfigProperties.LLC_SUFFIX, "0");
    flushThresholdUpdater = flushThresholdUpdateManager
        .getFlushThresholdUpdater(new PartitionLevelStreamConfig(REALTIME_TABLE_NAME, streamConfigMap));
    assertTrue(flushThresholdUpdater instanceof SegmentSizeBasedFlushThresholdUpdater);

    // called again with 0 flush size - same object as above
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS + StreamConfigProperties.LLC_SUFFIX);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "0");
    assertSame(flushThresholdUpdateManager
            .getFlushThresholdUpdater(new PartitionLevelStreamConfig(REALTIME_TABLE_NAME, streamConfigMap)),
        flushThresholdUpdater);

    // flush size reset to some number - default received, map cleared of segmentsize based
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "20000");
    flushThresholdUpdater = flushThresholdUpdateManager
        .getFlushThresholdUpdater(new PartitionLevelStreamConfig(REALTIME_TABLE_NAME, streamConfigMap));
    assertTrue(flushThresholdUpdater instanceof DefaultFlushThresholdUpdater);
    assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(), 20000);

    // optimal segment size set to invalid value. Default remains the same.
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "0");
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_DESIRED_SIZE, "Invalid");
    flushThresholdUpdater = flushThresholdUpdateManager
        .getFlushThresholdUpdater(new PartitionLevelStreamConfig(REALTIME_TABLE_NAME, streamConfigMap));
    assertTrue(flushThresholdUpdater instanceof SegmentSizeBasedFlushThresholdUpdater);
    assertEquals(((SegmentSizeBasedFlushThresholdUpdater) flushThresholdUpdater).getDesiredSegmentSizeBytes(),
        StreamConfig.getDefaultDesiredSegmentSizeBytes());

    // Clear the flush threshold updater for this table.
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "20000");
    flushThresholdUpdateManager
        .getFlushThresholdUpdater(new PartitionLevelStreamConfig(REALTIME_TABLE_NAME, streamConfigMap));

    // optimal segment size set to 500M
    long desiredSegSize = 500 * 1024 * 1024;
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "0");
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_DESIRED_SIZE, Long.toString(desiredSegSize));
    flushThresholdUpdater = flushThresholdUpdateManager
        .getFlushThresholdUpdater(new PartitionLevelStreamConfig(REALTIME_TABLE_NAME, streamConfigMap));
    assertTrue(flushThresholdUpdater instanceof SegmentSizeBasedFlushThresholdUpdater);
    assertEquals(((SegmentSizeBasedFlushThresholdUpdater) flushThresholdUpdater).getDesiredSegmentSizeBytes(),
        desiredSegSize);
    assertEquals(((SegmentSizeBasedFlushThresholdUpdater) flushThresholdUpdater).getAutotuneInitialRows(),
        DEFAULT_INITIAL_ROWS_THRESHOLD);

    // initial rows threshold
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_AUTOTUNE_INITIAL_ROWS, "500000");
    flushThresholdUpdater = new FlushThresholdUpdateManager()
        .getFlushThresholdUpdater(new PartitionLevelStreamConfig(REALTIME_TABLE_NAME, streamConfigMap));
    assertTrue(flushThresholdUpdater instanceof SegmentSizeBasedFlushThresholdUpdater);
    assertEquals(((SegmentSizeBasedFlushThresholdUpdater) flushThresholdUpdater).getAutotuneInitialRows(), 500_000);
  }

  /**
   * Tests change of config which enables SegmentSize based flush threshold updater, and tests the resetting of it back to default
   */
  @Test
  public void testUpdaterChange() {
    int tableFlushSize = 1_000_000;
    int partitionId = 0;
    long startOffset = 0;
    int maxNumPartitionsPerInstance = 4;

    // Initially we were using default flush threshold updation - verify that thresholds are as per default strategy
    LLCRealtimeSegmentZKMetadata metadata0 =
        getNextSegmentMetadata(partitionId, 0, startOffset, System.currentTimeMillis());

    FlushThresholdUpdater flushThresholdUpdater = new DefaultFlushThresholdUpdater(tableFlushSize);
    flushThresholdUpdater.updateFlushThreshold(metadata0, null, null, maxNumPartitionsPerInstance);

    assertEquals(metadata0.getSizeThresholdToFlushSegment(), 250_000);
    assertNull(metadata0.getTimeThresholdToFlushSegment());

    // before committing segment, we switched to size based updation - verify that new thresholds are set as per size based strategy
    flushThresholdUpdater =
        new SegmentSizeBasedFlushThresholdUpdater(DESIRED_SEGMENT_SIZE, DEFAULT_INITIAL_ROWS_THRESHOLD);

    startOffset += 1000;
    updateCommittingSegmentMetadata(metadata0, startOffset, 250_000);
    long committingSegmentSizeBytes = 180 * 1024 * 1024;
    CommittingSegmentDescriptor committingSegmentDescriptor =
        new CommittingSegmentDescriptor(metadata0.getSegmentName(), startOffset, committingSegmentSizeBytes);
    LLCRealtimeSegmentZKMetadata metadata1 =
        getNextSegmentMetadata(partitionId, 1, startOffset, System.currentTimeMillis());
    flushThresholdUpdater
        .updateFlushThreshold(metadata1, committingSegmentDescriptor, metadata0, maxNumPartitionsPerInstance);
    assertTrue(
        metadata1.getSizeThresholdToFlushSegment() != 0 && metadata1.getSizeThresholdToFlushSegment() != 250_000);

    // before committing we switched back to default strategy, verify that thresholds are set according to default logic
    flushThresholdUpdater = new DefaultFlushThresholdUpdater(tableFlushSize);

    startOffset += 1000;
    updateCommittingSegmentMetadata(metadata1, startOffset, metadata1.getSizeThresholdToFlushSegment());
    committingSegmentSizeBytes = 190 * 1024 * 1024;
    committingSegmentDescriptor =
        new CommittingSegmentDescriptor(metadata1.getSegmentName(), startOffset, committingSegmentSizeBytes);
    LLCRealtimeSegmentZKMetadata metadata2 =
        getNextSegmentMetadata(partitionId, 2, startOffset, System.currentTimeMillis());
    flushThresholdUpdater
        .updateFlushThreshold(metadata2, committingSegmentDescriptor, metadata1, maxNumPartitionsPerInstance);

    assertEquals(metadata2.getSizeThresholdToFlushSegment(), 250_000);
    assertNull(metadata2.getTimeThresholdToFlushSegment());
  }

  @Test
  public void testTimeThresholdInSegmentSizeBased() {
    int partitionId = 0;
    long startOffset = 0;
    long committingSegmentSizeBytes;
    CommittingSegmentDescriptor committingSegmentDescriptor;

    // initial segment
    LLCRealtimeSegmentZKMetadata metadata0 =
        getNextSegmentMetadata(partitionId, 0, startOffset, System.currentTimeMillis());
    SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater =
        new SegmentSizeBasedFlushThresholdUpdater(DESIRED_SEGMENT_SIZE, DEFAULT_INITIAL_ROWS_THRESHOLD);
    committingSegmentDescriptor = new CommittingSegmentDescriptor(metadata0.getSegmentName(), startOffset, 0);
    flushThresholdUpdater.updateFlushThreshold(metadata0, committingSegmentDescriptor, null, 0);
    assertEquals(metadata0.getSizeThresholdToFlushSegment(), flushThresholdUpdater.getAutotuneInitialRows());

    // next segment hit time threshold
    startOffset += 1000;
    updateCommittingSegmentMetadata(metadata0, startOffset, 98372);
    committingSegmentSizeBytes = 180 * 1024 * 1024;
    committingSegmentDescriptor =
        new CommittingSegmentDescriptor(metadata0.getSegmentName(), startOffset, committingSegmentSizeBytes);
    LLCRealtimeSegmentZKMetadata metadata1 =
        getNextSegmentMetadata(partitionId, 1, startOffset, System.currentTimeMillis());
    flushThresholdUpdater.updateFlushThreshold(metadata1, committingSegmentDescriptor, metadata0, 0);
    assertEquals(metadata1.getSizeThresholdToFlushSegment(),
        (int) (metadata0.getTotalRawDocs() * flushThresholdUpdater.getRowsMultiplierWhenTimeThresholdHit()));

    // now we hit rows threshold
    startOffset += 1000;
    updateCommittingSegmentMetadata(metadata1, startOffset, metadata1.getSizeThresholdToFlushSegment());
    committingSegmentSizeBytes = 240 * 1024 * 1024;
    committingSegmentDescriptor =
        new CommittingSegmentDescriptor(metadata1.getSegmentName(), startOffset, committingSegmentSizeBytes);
    LLCRealtimeSegmentZKMetadata metadata2 =
        getNextSegmentMetadata(partitionId, 2, startOffset, System.currentTimeMillis());
    flushThresholdUpdater.updateFlushThreshold(metadata2, committingSegmentDescriptor, metadata1, 0);
    assertTrue(metadata2.getSizeThresholdToFlushSegment() != metadata1.getSizeThresholdToFlushSegment());
  }

  @Test
  public void testMinThreshold() {
    int partitionId = 0;
    long startOffset = 0;
    CommittingSegmentDescriptor committingSegmentDescriptor;
    long now = System.currentTimeMillis();
    long seg0time = now - 1334_650;
    long seg1time = seg0time + 14_000;
    SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater =
        new SegmentSizeBasedFlushThresholdUpdater(DESIRED_SEGMENT_SIZE, DEFAULT_INITIAL_ROWS_THRESHOLD);

    // initial segment consumes only 15 rows, so next segment has 10k rows min.
    LLCRealtimeSegmentZKMetadata metadata0 = getNextSegmentMetadata(partitionId, 0, startOffset, seg0time);
    metadata0.setTotalRawDocs(15);
    metadata0.setSizeThresholdToFlushSegment(874_990);
    committingSegmentDescriptor = new CommittingSegmentDescriptor(metadata0.getSegmentName(), startOffset, 10_000);
    LLCRealtimeSegmentZKMetadata metadata1 = getNextSegmentMetadata(partitionId, 1, startOffset, seg1time);
    flushThresholdUpdater.updateFlushThreshold(metadata1, committingSegmentDescriptor, metadata0, 0);
    assertEquals(metadata1.getSizeThresholdToFlushSegment(), flushThresholdUpdater.getMinimumNumRowsThreshold());

    // seg1 also consumes 20 rows, so seg2 also gets 10k as threshold.
    LLCRealtimeSegmentZKMetadata metadata2 = getNextSegmentMetadata(partitionId, 2, startOffset, now);
    committingSegmentDescriptor =
        new CommittingSegmentDescriptor(metadata1.getSegmentName(), startOffset + 1000, 14_000);
    metadata1.setTotalRawDocs(25);
    flushThresholdUpdater.updateFlushThreshold(metadata2, committingSegmentDescriptor, metadata1, 0);
    assertEquals(metadata2.getSizeThresholdToFlushSegment(), flushThresholdUpdater.getMinimumNumRowsThreshold());
  }

  @Test
  public void testNonZeroPartitionUpdates() {
    long startOffset = 0;
    CommittingSegmentDescriptor committingSegmentDescriptor;
    long now = System.currentTimeMillis();
    long seg0time = now - 1334_650;
    long seg1time = seg0time + 14_000;
    SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater =
        new SegmentSizeBasedFlushThresholdUpdater(DESIRED_SEGMENT_SIZE, DEFAULT_INITIAL_ROWS_THRESHOLD);

    // Initial update is from partition 1
    LLCRealtimeSegmentZKMetadata metadata0 = getNextSegmentMetadata(1, 0, startOffset, seg0time);
    metadata0.setTotalRawDocs(1_234_000);
    metadata0.setSizeThresholdToFlushSegment(874_990);
    committingSegmentDescriptor = new CommittingSegmentDescriptor(metadata0.getSegmentName(), startOffset, 3_110_000);
    LLCRealtimeSegmentZKMetadata metadata1 = getNextSegmentMetadata(1, 1, startOffset, seg1time);
    assertEquals(flushThresholdUpdater.getLatestSegmentRowsToSizeRatio(), 0.0);
    flushThresholdUpdater.updateFlushThreshold(metadata1, committingSegmentDescriptor, metadata0, 0);
    double currentRatio = flushThresholdUpdater.getLatestSegmentRowsToSizeRatio();
    assertTrue(currentRatio > 0.0);

    // Next segment update from partition 1 does not change the ratio.
    LLCRealtimeSegmentZKMetadata metadata2 = getNextSegmentMetadata(1, 2, startOffset, now);
    committingSegmentDescriptor =
        new CommittingSegmentDescriptor(metadata1.getSegmentName(), startOffset + 1000, 256_000_000);
    metadata1.setTotalRawDocs(2_980_880);
    flushThresholdUpdater.updateFlushThreshold(metadata2, committingSegmentDescriptor, metadata1, 0);
    assertEquals(flushThresholdUpdater.getLatestSegmentRowsToSizeRatio(), currentRatio);

    // But if seg1 is from partition 0, the ratio is changed.
    metadata1 = getNextSegmentMetadata(0, 1, startOffset, seg1time);
    committingSegmentDescriptor.setSegmentName(metadata1.getSegmentName());
    flushThresholdUpdater.updateFlushThreshold(metadata2, committingSegmentDescriptor, metadata1, 0);
    assertNotEquals(flushThresholdUpdater.getLatestSegmentRowsToSizeRatio(), currentRatio);
  }
}
