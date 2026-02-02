/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.opensearch.common.settings.Settings;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for FlushModeResolver settings and decideFlushMode logic.
 */
public class FlushModeResolverTests extends OpenSearchTestCase {

    public void testSettingsDefaults() {
        assertEquals(100_000L, FlushModeResolver.STREAMING_MAX_ESTIMATED_BUCKET_COUNT.getDefault(Settings.EMPTY).longValue());
        assertEquals(0.01, FlushModeResolver.STREAMING_MIN_CARDINALITY_RATIO.getDefault(Settings.EMPTY).doubleValue(), 0.001);
        assertEquals(1000L, FlushModeResolver.STREAMING_MIN_ESTIMATED_BUCKET_COUNT.getDefault(Settings.EMPTY).longValue());
        assertEquals(1000, FlushModeResolver.STREAMING_AGGREGATION_MIN_SEGMENT_SIZE_SETTING.getDefault(Settings.EMPTY).intValue());
    }

    public void testDecideFlushModeNonStreamable() {
        StreamingCostMetrics nonStreamable = StreamingCostMetrics.nonStreamable();
        FlushMode result = FlushModeResolver.decideFlushMode(nonStreamable, FlushMode.PER_SHARD, 100_000);
        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testDecideFlushModeStreamable() {
        // topN=10 <= maxBucketCount=100_000, should stream
        StreamingCostMetrics streamable = new StreamingCostMetrics(true, 10);
        FlushMode result = FlushModeResolver.decideFlushMode(streamable, FlushMode.PER_SHARD, 100_000);
        assertEquals(FlushMode.PER_SEGMENT, result);
    }

    public void testDecideFlushModeTopNExceedsMax() {
        // topN=200_000 > maxBucketCount=100_000, should not stream
        StreamingCostMetrics highTopN = new StreamingCostMetrics(true, 200_000);
        FlushMode result = FlushModeResolver.decideFlushMode(highTopN, FlushMode.PER_SHARD, 100_000);
        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testDecideFlushModeTopNExactlyAtMax() {
        // topN=100_000 == maxBucketCount=100_000, should stream (<=)
        StreamingCostMetrics exactMatch = new StreamingCostMetrics(true, 100_000);
        FlushMode result = FlushModeResolver.decideFlushMode(exactMatch, FlushMode.PER_SHARD, 100_000);
        assertEquals(FlushMode.PER_SEGMENT, result);
    }

    public void testDecideFlushModeNeutralMetrics() {
        // Neutral metrics have topN=1, which is <= any reasonable max, so should stream
        StreamingCostMetrics neutral = StreamingCostMetrics.neutral();
        FlushMode result = FlushModeResolver.decideFlushMode(neutral, FlushMode.PER_SHARD, 100_000);
        assertEquals(FlushMode.PER_SEGMENT, result);
    }

    public void testDecideFlushModeSmallTopN() {
        // Very small topN (cardinality case), should stream
        StreamingCostMetrics smallTopN = new StreamingCostMetrics(true, 1);
        FlushMode result = FlushModeResolver.decideFlushMode(smallTopN, FlushMode.PER_SHARD, 100_000);
        assertEquals(FlushMode.PER_SEGMENT, result);
    }

    public void testIsStreamableSetsStreamingEnabledOnTermsAggregation() {
        TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("test_terms");
        termsAgg.field("category");

        // Before calling isStreamable, type should be "terms"
        assertEquals("terms", termsAgg.getType());

        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(termsAgg);

        // isStreamable should return true and set streamingEnabled on the TermsAggregationBuilder
        assertTrue(FlushModeResolver.isStreamable(aggBuilder));

        // After isStreamable, type should be "streaming_terms"
        assertEquals("streaming_terms", termsAgg.getType());
    }

    public void testIsStreamableWithSubAggregationsSetsStreamingEnabled() {
        TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("test_terms");
        termsAgg.field("category");
        termsAgg.subAggregation(new MaxAggregationBuilder("max_price").field("price"));

        assertEquals("terms", termsAgg.getType());

        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(termsAgg);

        assertTrue(FlushModeResolver.isStreamable(aggBuilder));
        assertEquals("streaming_terms", termsAgg.getType());
    }

    public void testIsStreamableReturnsFalseForNonTermsAggregation() {
        MaxAggregationBuilder maxAgg = new MaxAggregationBuilder("max_value");
        maxAgg.field("value");

        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(maxAgg);

        // Non-terms aggregation at top level should not be streamable
        assertFalse(FlushModeResolver.isStreamable(aggBuilder));
    }

    public void testIsStreamableReturnsFalseForNullOrEmptyAggregations() {
        assertFalse(FlushModeResolver.isStreamable(null));
        assertFalse(FlushModeResolver.isStreamable(new AggregatorFactories.Builder()));
    }
}
