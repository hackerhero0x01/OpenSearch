/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream.nodes;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for streaming data from nodes
 */
/**
 * Example class
 */
/** Example */
public class StreamNodesDataRequest extends BaseNodesRequest<StreamNodesDataRequest> {
    private int count = 5;
    private long delayMs = 500;

    /**
     * Constructor from stream input
     * @param in stream input
     * @throws IOException if read fails
     */
    /**
     * Constructor
     */
    /** Method */
    public StreamNodesDataRequest(StreamInput in) throws IOException {
        super(in);
        count = in.readInt();
        delayMs = in.readLong();
    }

    /**
     * Constructor with node IDs
     * @param nodesIds node IDs
     */
    /**
     * Constructor
     */
    /** Method */
    public StreamNodesDataRequest(String... nodesIds) {
        super(nodesIds);
    }

    /**
     * Set count
     * @param count count
     * @return this
     */
    /**
     * Method
     * @return result
     */
    /** Method */
    public StreamNodesDataRequest count(int count) {
        this.count = count;
        return this;
    }

    /**
     * Set delay in milliseconds
     * @param delayMs delay
     * @return this
     */
    /**
     * Method
     * @return result
     */
    /** Method */
    public StreamNodesDataRequest delayMs(long delayMs) {
        this.delayMs = delayMs;
        return this;
    }

    @Override
    /**
     * Method
     * @return result
     */
    /** Method */
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(count);
        out.writeLong(delayMs);
    }

    /**
     * Get count
     * @return count
     */
    /**
     * Method
     * @return result
     */
    /** Method */
    public int getCount() {
        return count;
    }

    /**
     * Get delay in milliseconds
     * @return delay
     */
    /**
     * Method
     * @return result
     */
    /** Method */
    public long getDelayMs() {
        return delayMs;
    }
}
