package com.flipkart.varadhi.web.core.authz;

import com.flipkart.varadhi.entities.auth.Role;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class DefaultAuthorizationConfig {
    /**
     * role_id to Role{role_id, [permissions...]} mappings
     */
    private Map<String, Role> roleDefinitions;

    private MetaStoreOptions metaStoreOptions;

    private List<String> superUsers;
}
