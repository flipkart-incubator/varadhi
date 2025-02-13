package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.web.Extensions;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


@Slf4j
@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class OrgService {
    private final MetaStore metaStore;

    public OrgService(MetaStore metaStore) {
        this.metaStore = metaStore;
    }

    public Org createOrg(Org org) {
        metaStore.createOrg(org);
        return org;
    }

    public List<Org> getOrgs() {
        return metaStore.getOrgs();
    }

    public Org getOrg(String orgName) {
        return metaStore.getOrg(orgName);
    }


    public void deleteOrg(String orgName) {
        List<String> teamsInOrg = metaStore.getTeamNames(orgName);
        if (teamsInOrg.size() > 0) {
            throw new InvalidOperationForResourceException(
                String.format("Can not delete Org(%s) as it has associated Team(s).", orgName)
            );
        }
        metaStore.deleteOrg(orgName);
    }
}
