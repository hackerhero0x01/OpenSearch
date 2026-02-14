/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream.nodes;

import org.opensearch.action.StreamActionListener;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.example.stream.StreamTransportExamplePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.node.NodeClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 3)
public class StreamNodesTransportExampleIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("opensearch.experimental.feature.transport.stream.enabled", true)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(StreamTransportExamplePlugin.class, FlightStreamPlugin.class);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    @AwaitsFix(bugUrl = "flaky")
    public void testStreamNodesAction() throws Exception {
        NodeClient nodeClient = internalCluster().getInstance(NodeClient.class);

        StreamNodesDataRequest request = new StreamNodesDataRequest("_all").count(3).delayMs(100);

        CountDownLatch latch = new CountDownLatch(1);
        List<NodeStreamDataResponse> allResponses = new ArrayList<>();

        nodeClient.executeStream(StreamNodesDataAction.INSTANCE, request, new StreamActionListener<StreamNodesDataResponse>() {
            @Override
            public void onStreamResponse(StreamNodesDataResponse response) {
                synchronized (allResponses) {
                    allResponses.addAll(response.getNodes());
                }
            }

            @Override
            public void onFinalResponse() {
                latch.countDown();
            }

            @Override
            public void onStreamFailure(Exception e) {
                logger.error("Stream failed", e);
                fail("Stream should not fail: " + e.getMessage());
                latch.countDown();
            }
        });

        assertTrue("Stream should complete", latch.await(30, TimeUnit.SECONDS));
        int numNodes = internalCluster().size();
        assertEquals("Should receive 3 items per node * " + numNodes + " nodes", 3 * numNodes, allResponses.size());
    }
}
