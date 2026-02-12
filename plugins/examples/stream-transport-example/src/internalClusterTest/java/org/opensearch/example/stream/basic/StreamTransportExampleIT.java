/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream.basic;

import org.opensearch.action.StreamActionListener;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.example.stream.StreamTransportExamplePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.node.NodeClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 2, maxNumDataNodes = 2)
public class StreamTransportExampleIT extends OpenSearchIntegTestCase {
    @Override
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().ensureAtLeastNumDataNodes(2);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(StreamTransportExamplePlugin.class, FlightStreamPlugin.class);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    @AwaitsFix(bugUrl = "flaky")
    public void testStreamTransportActionWithClient() throws Exception {
        // Ensure cluster is ready
        ensureGreen();

        NodeClient nodeClient = internalCluster().getInstance(NodeClient.class);

        List<StreamDataResponse> responses = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        StreamDataRequest request = new StreamDataRequest(3, 1);

        nodeClient.executeStream(StreamDataAction.INSTANCE, request, new StreamActionListener<StreamDataResponse>() {
            @Override
            public void onStreamResponse(StreamDataResponse response) {
                responses.add(response);
            }

            @Override
            public void onFinalResponse() {
                latch.countDown();
            }

            @Override
            public void onStreamFailure(Exception e) {
                error.set(e);
                latch.countDown();
            }
        });

        assertTrue("Timeout waiting for stream completion", latch.await(5, TimeUnit.SECONDS));

        if (error.get() != null) {
            fail("Stream failed: " + error.get().getMessage());
        }

        assertEquals(3, responses.size());
        assertEquals("Stream data item 1", responses.get(0).getMessage());
        assertEquals("Stream data item 2", responses.get(1).getMessage());
        assertEquals("Stream data item 3", responses.get(2).getMessage());
        assertTrue(responses.get(2).isLast());
    }
}
