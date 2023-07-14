package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.VaradhiEntity;

public interface VaradhiEntityService<T extends VaradhiEntity> {

    T create(T varadhiEntity);

    boolean exists(String entityName);
}
