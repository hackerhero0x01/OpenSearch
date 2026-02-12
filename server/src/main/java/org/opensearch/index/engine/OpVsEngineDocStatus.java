/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

/**
 * the status of the current doc version in lucene, compared to the version in an incoming
 * operation
 */
public enum OpVsEngineDocStatus {
    /** the op is more recent than the one that last modified the doc found in lucene*/
    OP_NEWER,
    /** the op is older or the same as the one that last modified the doc found in lucene*/
    OP_STALE_OR_EQUAL,
    /** no doc was found in lucene */
    DOC_NOT_FOUND
}
