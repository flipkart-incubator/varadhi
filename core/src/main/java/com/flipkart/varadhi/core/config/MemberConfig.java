package com.flipkart.varadhi.core.config;

import com.flipkart.varadhi.core.cluster.ComponentKind;
import jakarta.validation.constraints.NotEmpty;
import lombok.Data;

@Data
public class MemberConfig {

    /**
     * Special region name that is accepted during cluster bootstrapping when no regions have been registered in the
     * metastore yet. Once at least one region exists in the metastore every node must be configured with a valid,
     * registered region name.
     */
    public static final String BOOTSTRAP_REGION = "default";

    @NotEmpty
    private ComponentKind[] roles;
    private int clusterPort;
    private int maxQps;
    private int networkMBps;
    private int cpuCount;

    /**
     * Deployment region this node belongs to.
     * <p>
     * Must match a region registered in the metastore. During initial cluster bootstrapping (no regions in the
     * metastore yet) the value {@value #BOOTSTRAP_REGION} is accepted so the first node can start without
     * pre-existing region metadata. Actual region names must be provisioned via the Region API and this field must be
     * updated before the node restarts.
     */
    @NotEmpty
    private String region;
}
