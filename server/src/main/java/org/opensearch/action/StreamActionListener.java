/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Listener for streaming action responses
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface StreamActionListener<Response> {
    /**
     * Called for each batch response
     */
    void onStreamResponse(Response response);

    /**
     * Called when stream completes successfully
     */
    void onFinalResponse();

    /**
     * Called on stream failure
     */
    void onStreamFailure(Exception e);
}
