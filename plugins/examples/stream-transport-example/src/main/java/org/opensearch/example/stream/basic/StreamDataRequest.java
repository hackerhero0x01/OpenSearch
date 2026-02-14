/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream.basic;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Example class
 */
/** Example */
public class StreamDataRequest extends ActionRequest {
    private int count = 10;
    private long delayMs = 1000;

    /**
     * Constructor
     */
       /** Method */
    public StreamDataRequest() {}

    /**
     * Constructor
     */
    /** Method */
    public StreamDataRequest(StreamInput in) throws IOException {
        super(in);
        count = in.readInt();
        delayMs = in.readLong();
    }

    /**
     * Constructor
     */
    /** Method */
    public StreamDataRequest(int count, long delayMs) {
        this.count = count;
        this.delayMs = delayMs;
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

    @Override
    /**
     * Method
     * @return result
     */
    /** Method */
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Method
     * @return result
     */
    /** Method */
    public int getCount() {
        return count;
    }

    /**
     * Method
     * @return result
     */
    /** Method */
    public long getDelayMs() {
        return delayMs;
    }
}
