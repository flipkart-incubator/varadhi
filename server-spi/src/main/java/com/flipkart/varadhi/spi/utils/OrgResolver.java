package com.flipkart.varadhi.spi.utils;

import com.flipkart.varadhi.entities.Org;

public interface OrgResolver {
    Org resolve(String orgName);
}
