package com.flipkart.varadhi.controller.events;

import com.flipkart.varadhi.entities.ResourceOperation;

/**
 * Represents the result of resolving a resource's current state.
 * This record encapsulates both the resolved state and the operation
 * to be performed based on the resolution outcome.
 *
 * <h2>Resolution Cases</h2>
 * <ul>
 *   <li>Resource exists: Returns state with UPSERT operation</li>
 *   <li>Resource not found: Returns null state with INVALIDATE operation</li>
 *   <li>No state required: Returns null state with UPSERT operation</li>
 * </ul>
 *
 * @param state The current state of the resource, may be null
 * @param operation The operation to perform based on state resolution
 */
public record StateResolutionResult(Object state, ResourceOperation operation) {

    /**
     * Creates a result for an existing resource with its current state.
     *
     * @param state The current state of the resource
     * @return A new StateResolutionResult with UPSERT operation
     */
    public static StateResolutionResult of(Object state) {
        return new StateResolutionResult(state, ResourceOperation.UPSERT);
    }

    /**
     * Creates a result for a non-existent resource.
     *
     * @return A new StateResolutionResult with INVALIDATE operation
     */
    public static StateResolutionResult notFound() {
        return new StateResolutionResult(null, ResourceOperation.INVALIDATE);
    }

    /**
     * Creates a result for cases where resource state is not required.
     *
     * @return A new StateResolutionResult with UPSERT operation and null state
     */
    public static StateResolutionResult noStateRequired() {
        return new StateResolutionResult(null, ResourceOperation.UPSERT);
    }
}
