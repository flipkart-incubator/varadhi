package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.entities.filters.Condition;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.spi.db.OrgMetaStore;
import com.flipkart.varadhi.spi.db.TeamMetaStore;
import com.flipkart.varadhi.web.Extensions;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


@Slf4j
@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class OrgService {
    private final OrgMetaStore orgMetaStore;
    private final TeamMetaStore teamMetaStore;

    public OrgService(OrgMetaStore orgMetaStore, TeamMetaStore teamMetaStore) {
        this.orgMetaStore = orgMetaStore;
        this.teamMetaStore = teamMetaStore;
    }

    public Org createOrg(Org org) {
        orgMetaStore.createOrg(org);
        return org;
    }

    public List<Org> getOrgs() {
        return orgMetaStore.getOrgs();
    }

    public Org getOrg(String orgName) {
        return orgMetaStore.getOrg(orgName);
    }


    public void deleteOrg(String orgName) {
        List<String> teamsInOrg = teamMetaStore.getTeamNames(orgName);
        if (teamsInOrg.size() > 0) {
            throw new InvalidOperationForResourceException(
                String.format("Can not delete Org(%s) as it has associated Team(s).", orgName)
            );
        }
        orgMetaStore.deleteOrg(orgName);
    }

    public Condition getFilter(String orgName, String filterName) {
        return orgMetaStore.getOrgFilter(orgName).getFilters().get(filterName);
    }

    public OrgFilters getAllFilters(String orgName) {
        return orgMetaStore.getOrgFilter(orgName);
    }

    public boolean filterExists(String orgName, String filterName) {
        return orgMetaStore.getOrgFilter(orgName).getFilters().get(filterName) != null;
    }

    public void updateFilter(String orgName, String filterName, OrgFilters orgFilters) {
        orgMetaStore.updateOrgFilter(orgName, filterName, orgFilters);
    }

    public OrgFilters createFilter(String orgName, OrgFilters orgFilter) {
        return orgMetaStore.createOrgFilter(orgName, orgFilter);
    }

    public void deleteFilter(String orgName) {
        orgMetaStore.deleteOrgFilter(orgName);
    }
}
