package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.org.OrgOperations;
import com.flipkart.varadhi.spi.db.team.TeamOperations;
import com.flipkart.varadhi.web.Extensions;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class OrgService {
    private final OrgOperations orgOperations;
    private final TeamOperations teamOperations;

    public OrgService(OrgOperations orgOperations, TeamOperations teamOperations) {
        this.orgOperations = orgOperations;
        this.teamOperations = teamOperations;
    }

    public Org createOrg(Org org) {
        orgOperations.createOrg(org);
        return org;
    }

    public List<Org> getOrgs() {
        return orgOperations.getOrgs();
    }

    public Org getOrg(String orgName) {
        return orgOperations.getOrg(orgName);
    }


    public void deleteOrg(String orgName) {
        List<String> teamsInOrg = teamOperations.getTeamNames(orgName);
        if (teamsInOrg.size() > 0) {
            throw new InvalidOperationForResourceException(
                    String.format("Can not delete Org(%s) as it has associated Team(s).", orgName)
            );
        }
        orgOperations.deleteOrg(orgName);
    }
}
