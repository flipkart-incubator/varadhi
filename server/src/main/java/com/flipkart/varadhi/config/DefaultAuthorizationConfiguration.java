package com.flipkart.varadhi.config;

import com.flipkart.varadhi.entities.auth.Role;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import lombok.Data;

import java.util.Map;

@Data
public class DefaultAuthorizationConfiguration {
    /**
     * role_id to Role{role_id, [permissions...]} mappings
     */
    private Map<String, Role> roleDefinitions;

    private MetaStoreOptions metaStoreOptions;
}
