/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.StreamActionListener;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.StreamTransportService;

/**
 * Base class for streaming transport actions
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class StreamTransportAction<Request extends ActionRequest, Response extends ActionResponse> extends TransportAction<
    Request,
    Response> {

    protected final StreamTransportService streamTransportService;

    protected StreamTransportAction(String actionName, StreamTransportService streamTransportService, ActionFilters actionFilters) {
        super(actionName, actionFilters, streamTransportService.getTaskManager());
        this.streamTransportService = streamTransportService;
    }

    @Override
    protected final void doExecute(Task task, Request request, ActionListener<Response> listener) {
        listener.onFailure(new UnsupportedOperationException("Use executeStream() for streaming actions"));
    }

    /**
     * Execute streaming request
     */
    public abstract void executeStream(Request request, StreamActionListener<Response> listener);
}
