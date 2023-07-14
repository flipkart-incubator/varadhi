package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.OrgEntity;
import com.flipkart.varadhi.entities.VaradhiEntity;
import com.flipkart.varadhi.entities.VaradhiEntityType;

public class VaradhiEntityUtils {
    public static Class<? extends VaradhiEntity> fetchEntityClassFromType(VaradhiEntityType varadhiEntityType) {

        if (varadhiEntityType.equals(VaradhiEntityType.ORG)) {
            return OrgEntity.class;
        }

        return null;
    }

}
