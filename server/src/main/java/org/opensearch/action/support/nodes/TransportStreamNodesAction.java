/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.nodes;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.StreamActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.StreamTransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for streaming transport nodes actions
 * <p>
 * This class provides a framework for executing streaming operations across multiple nodes.
 * Subclasses must implement {@link #nodeStreamOperation} to handle the streaming logic on each node.
 *
 * @opensearch.internal
 */
public abstract class TransportStreamNodesAction<
    NodesRequest extends BaseNodesRequest<NodesRequest>,
    NodesResponse extends BaseNodesResponse,
    NodeRequest extends TransportRequest,
    NodeResponse extends BaseNodeResponse> extends StreamTransportAction<NodesRequest, NodesResponse> {

    protected final ThreadPool threadPool;
    protected final ClusterService clusterService;
    protected final StreamTransportService streamTransportService;
    protected final Class<NodeResponse> nodeResponseClass;
    protected final String transportNodeAction;
    private final String nodeExecutor;

    protected TransportStreamNodesAction(
        String actionName,
        ThreadPool threadPool,
        ClusterService clusterService,
        StreamTransportService streamTransportService,
        ActionFilters actionFilters,
        Writeable.Reader<NodesRequest> request,
        Writeable.Reader<NodeRequest> nodeRequest,
        String nodeExecutor,
        Class<NodeResponse> nodeResponseClass
    ) {
        super(actionName, streamTransportService, actionFilters);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.streamTransportService = streamTransportService;
        this.nodeResponseClass = nodeResponseClass;
        this.nodeExecutor = nodeExecutor;
        this.transportNodeAction = actionName + "[n]";

        streamTransportService.registerRequestHandler(transportNodeAction, nodeExecutor, nodeRequest, this::handleNodeRequest);
    }

    @Override
    public void executeStream(NodesRequest request, StreamActionListener<NodesResponse> listener) {
        if (request.concreteNodes() == null) {
            resolveRequest(request, clusterService.state());
            assert request.concreteNodes() != null;
        }

        DiscoveryNode[] nodes = request.concreteNodes();
        request.setConcreteNodes(null);

        if (nodes.length == 0) {
            listener.onFinalResponse();
            return;
        }

        AtomicInteger counter = new AtomicInteger(nodes.length);
        List<FailedNodeException> failures = new ArrayList<>();

        for (int i = 0; i < nodes.length; i++) {
            final DiscoveryNode node = nodes[i];

            try {
                NodeRequest nodeRequest = newNodeRequest(request);
                streamTransportService.sendRequest(
                    node,
                    transportNodeAction,
                    nodeRequest,
                    TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
                    new StreamTransportResponseHandler<NodeResponse>() {
                        @Override
                        public void handleStreamResponse(StreamTransportResponse<NodeResponse> streamResponse) {
                            try {
                                NodeResponse response;
                                while ((response = streamResponse.nextResponse()) != null) {
                                    listener.onStreamResponse(newResponse(request, List.of(response), new ArrayList<>(failures)));
                                }
                                streamResponse.close();
                            } catch (Exception e) {
                                streamResponse.cancel("Client error", e);
                                synchronized (failures) {
                                    failures.add(new FailedNodeException(node.getId(), "Stream processing failed", e));
                                }
                            } finally {
                                if (counter.decrementAndGet() == 0) {
                                    listener.onFinalResponse();
                                }
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            synchronized (failures) {
                                failures.add(new FailedNodeException(node.getId(), "Request failed", exp));
                            }
                            if (counter.decrementAndGet() == 0) {
                                listener.onFinalResponse();
                            }
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public NodeResponse read(StreamInput in) throws IOException {
                            return newNodeResponse(in);
                        }
                    }
                );
            } catch (Exception e) {
                synchronized (failures) {
                    failures.add(new FailedNodeException(node.getId(), "Failed to send request", e));
                }
                if (counter.decrementAndGet() == 0) {
                    listener.onFinalResponse();
                }
            }
        }
    }

    /**
     * Handle streaming request on individual node
     */
    private void handleNodeRequest(NodeRequest request, TransportChannel channel, Task task) throws IOException {
        try {
            nodeStreamOperation(request, channel, task);
        } catch (Exception e) {
            channel.sendResponse(e);
        }
    }

    /**
     * Implement this to handle streaming on each node.
     * <p>
     * Use {@link TransportChannel#sendResponseBatch} to send batched responses
     * and {@link TransportChannel#completeStream} to signal completion.
     *
     * @param request the node request
     * @param channel the transport channel for sending responses
     * @param task    the task
     * @throws IOException if an I/O error occurs
     */
    protected abstract void nodeStreamOperation(NodeRequest request, TransportChannel channel, Task task) throws IOException;

    /**
     * Create node request from nodes request
     */
    protected abstract NodeRequest newNodeRequest(NodesRequest request);

    /**
     * Read node response from stream
     */
    protected abstract NodeResponse newNodeResponse(StreamInput in) throws IOException;

    /**
     * Create final aggregated response
     */
    protected abstract NodesResponse newResponse(NodesRequest request, List<NodeResponse> responses, List<FailedNodeException> failures);

    protected void resolveRequest(NodesRequest request, ClusterState clusterState) {
        assert request.concreteNodes() == null : "request concreteNodes shouldn't be set";
        String[] nodesIds = clusterState.nodes().resolveNodes(request.nodesIds());
        request.setConcreteNodes(Arrays.stream(nodesIds).map(clusterState.nodes()::get).toArray(DiscoveryNode[]::new));
    }
}
