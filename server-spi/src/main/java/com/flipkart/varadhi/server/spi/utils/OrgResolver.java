package com.flipkart.varadhi.server.spi.utils;

import com.flipkart.varadhi.entities.Org;

public interface OrgResolver {
    Org resolve(String orgName);
}
