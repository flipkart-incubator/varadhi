package com.flipkart.varadhi.services;

import com.flipkart.varadhi.db.MetaStore;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.DuplicateResourceException;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class OrgService implements VaradhiEntityService<OrgEntity> {
    private final MetaStore metaStore;

    private static final VaradhiEntityType VARADHI_ENTITY_TYPE = VaradhiEntityType.ORG;

    public OrgService(MetaStore metaStore) {
        this.metaStore = metaStore;
    }

    @Override
    public OrgEntity create(OrgEntity orgEntity) {
        log.info("Creating Org entity {}", orgEntity.getName());

        if (exists(orgEntity.getName())) {
            log.error("Specified Org({}) already exists.", orgEntity.getName());
            throw new DuplicateResourceException(String.format("Specified Org(%s) already exists.",
                    orgEntity.getName()));
        }

        return (OrgEntity) metaStore.createVaradhiEntity(VARADHI_ENTITY_TYPE, orgEntity);
    }

    @Override
    public boolean exists(String entityName) {
        return metaStore.checkVaradhiEntityExists(VaradhiEntityType.ORG, entityName);
    }

}
