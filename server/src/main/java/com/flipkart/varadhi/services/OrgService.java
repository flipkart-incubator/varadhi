package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.entities.filters.Condition;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.spi.db.OrgStore;
import com.flipkart.varadhi.spi.db.TeamStore;
import com.flipkart.varadhi.web.Extensions;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


@Slf4j
@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class OrgService {
    private final OrgStore orgStore;
    private final TeamStore teamStore;

    public OrgService(OrgStore orgStore, TeamStore teamStore) {
        this.orgStore = orgStore;
        this.teamStore = teamStore;
    }

    public Org createOrg(Org org) {
        orgStore.create(org);
        return org;
    }

    public List<Org> getOrgs() {
        return orgStore.getAll();
    }

    public Org getOrg(String orgName) {
        return orgStore.get(orgName);
    }


    public void deleteOrg(String orgName) {
        List<String> teamsInOrg = teamStore.getAllNames(orgName);
        if (teamsInOrg.size() > 0) {
            throw new InvalidOperationForResourceException(
                String.format("Can not delete Org(%s) as it has associated Team(s).", orgName)
            );
        }
        orgStore.delete(orgName);
    }

    public Condition getFilter(String orgName, String filterName) {
        return orgStore.getFilter(orgName).getFilters().get(filterName);
    }

    public OrgFilters getAllFilters(String orgName) {
        return orgStore.getFilter(orgName);
    }

    public boolean filterExists(String orgName, String filterName) {
        return orgStore.getFilter(orgName).getFilters().get(filterName) != null;
    }

    public void updateFilter(String orgName, OrgFilters orgFilters) {
        orgStore.updateFilter(orgName, orgFilters);
    }

    public OrgFilters createFilter(String orgName, OrgFilters orgFilter) {
        return orgStore.createFilter(orgName, orgFilter);
    }

    public void deleteFilter(String orgName) {
        orgStore.deleteFilter(orgName);
    }
}
