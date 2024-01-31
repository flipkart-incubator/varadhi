package com.flipkart.varadhi.core.cluster;

/**
 * TODO: need to see if the id needs to be separate from the 'ip' & 'hostname'. Implementer needs to address this.
 * 'id' role is unique, and is the identifier for a node, that can be used as the address in eventBus.
 */
public record NodeInfo(
        String id,
        String ip,
        String hostname,
        NodeResources resources
) {
}
