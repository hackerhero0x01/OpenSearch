/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream.basic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.StreamActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.StreamTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;

/**
 * Demonstrates streaming transport action that sends multiple responses for a single request
 */
/**
 * Example class
 */
/** Example */
public class TransportStreamDataAction extends StreamTransportAction<StreamDataRequest, StreamDataResponse> {

    private static final Logger logger = LogManager.getLogger(TransportStreamDataAction.class);
    private final TransportService transportService;

    /**
     * Constructor - registers streaming handler
     * @param streamTransportService the stream transport service
     * @param transportService the regular transport service
     * @param actionFilters action filters
     */
    @Inject
    /**
     * Constructor
     */
    /** Method */
    public TransportStreamDataAction(
        StreamTransportService streamTransportService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(StreamDataAction.NAME, streamTransportService, actionFilters);
        this.transportService = transportService;

        // Register handler for streaming requests
        streamTransportService.registerRequestHandler(
            StreamDataAction.NAME,
            ThreadPool.Names.GENERIC,
            StreamDataRequest::new,
            this::handleStreamRequest
        );
    }

    @Override
    /**
     * Method
     * @return result
     */
    /** Method */
    public void executeStream(StreamDataRequest request, StreamActionListener<StreamDataResponse> listener) {
        try {
            // Use transportService's local node (not streamTransportService's) for proper connection handling
            DiscoveryNode targetNode = transportService.getLocalNode();

            streamTransportService.sendRequest(
                targetNode,
                StreamDataAction.NAME,
                request,
                TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
                new StreamTransportResponseHandler<StreamDataResponse>() {
                    @Override
                    public void handleStreamResponse(StreamTransportResponse<StreamDataResponse> streamResponse) {
                        try {
                            StreamDataResponse response;
                            while ((response = streamResponse.nextResponse()) != null) {
                                listener.onStreamResponse(response);
                            }
                            streamResponse.close();
                            listener.onFinalResponse();
                        } catch (Exception e) {
                            streamResponse.cancel("Client error", e);
                            listener.onStreamFailure(e);
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        listener.onStreamFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public StreamDataResponse read(StreamInput in) throws IOException {
                        return new StreamDataResponse(in);
                    }
                }
            );
        } catch (Exception e) {
            listener.onStreamFailure(e);
        }
    }

    /**
     * Handles streaming request by sending multiple batched responses
     */
    private void handleStreamRequest(StreamDataRequest request, TransportChannel channel, Task task) throws IOException {
        try {
            // Send multiple responses
            for (int i = 1; i <= request.getCount(); i++) {
                StreamDataResponse response = new StreamDataResponse("Stream data item " + i, i, i == request.getCount());

                channel.sendResponseBatch(response);

                if (i < request.getCount() && request.getDelayMs() > 0) {
                    Thread.sleep(request.getDelayMs());
                }
            }

            channel.completeStream();

        } catch (StreamException e) {
            if (e.getErrorCode() == StreamErrorCode.CANCELLED) {
                logger.info("Client cancelled stream: {}", e.getMessage());
            } else {
                channel.sendResponse(e);
            }
        } catch (Exception e) {
            channel.sendResponse(e);
        }
    }
}
