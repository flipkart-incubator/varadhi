package com.flipkart.varadhi.services;

import com.flipkart.varadhi.db.MetaStore;
import com.flipkart.varadhi.entities.VaradhiEntityType;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;

import java.util.HashMap;
import java.util.Map;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class VaradhiEntityServiceFactory {
    Map<VaradhiEntityType, VaradhiEntityService> varadhiEntityServiceMap;

    public VaradhiEntityServiceFactory(MetaStore metaStore) {
        varadhiEntityServiceMap = new HashMap<>();
        varadhiEntityServiceMap.put(VaradhiEntityType.ORG, new OrgService(metaStore));
        //TODO : add for other entities, by iterating over enum
    }
    public VaradhiEntityService getVaradhiEntityService(VaradhiEntityType varadhiEntityType) {
        return varadhiEntityServiceMap.get(varadhiEntityType);
    }
}
