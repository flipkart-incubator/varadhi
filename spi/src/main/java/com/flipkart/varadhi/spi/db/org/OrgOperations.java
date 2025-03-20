package com.flipkart.varadhi.spi.db.org;

import com.flipkart.varadhi.entities.Org;

import java.util.List;

public interface OrgOperations {
    void createOrg(Org org);

    Org getOrg(String orgName);

    List<Org> getOrgs();

    boolean checkOrgExists(String orgName);

    void deleteOrg(String orgName);
}
