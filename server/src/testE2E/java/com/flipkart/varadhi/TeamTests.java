package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Team;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TeamTests extends E2EBase {
    Org org1, org2, org3;
    Team org1Team1, org1Team2, org2Team1, org2Team2, org3Team1;


    @BeforeEach
    public void setup() {
        org1 = Org.of("org1");
        org2 = Org.of("org2");
        org3 = Org.of("org3");
        org1Team1 = Team.of("team1", org1.getName());
        org1Team2 = Team.of("team2", org1.getName());
        org2Team1 = Team.of("team2", org2.getName());
        org2Team2 = Team.of("team3", org2.getName());
        org3Team1 = Team.of("team1", org3.getName());

        makeCreateRequest(getOrgsUri(), org1, 200);
        makeCreateRequest(getOrgsUri(), org2, 200);
    }

    @AfterEach
    public void cleanup() {
        cleanupOrgs(List.of(org1, org2, org3));
    }

    @Test
    public void testTeamCRUD() {
        Team c_org1Team1 = makeCreateRequest(getTeamsUri(org1Team1.getOrg()), org1Team1, 200);
        Team c_org1Team2 = makeCreateRequest(getTeamsUri(org1Team2.getOrg()), org1Team2, 200);
        Team c_org2Team1 = makeCreateRequest(getTeamsUri(org2Team1.getOrg()), org2Team1, 200);
        Team c_org2Team2 = makeCreateRequest(getTeamsUri(org2Team2.getOrg()), org2Team2, 200);
        Assertions.assertEquals(org1Team1, c_org1Team1);
        Assertions.assertEquals(org1Team2, c_org1Team2);
        Assertions.assertEquals(org2Team1, c_org2Team1);
        Assertions.assertEquals(org2Team2, c_org2Team2);

        makeCreateRequest(
            getTeamsUri(org1Team1.getOrg()),
            org1Team1,
            409,
            String.format("Team(%s) already exists.", org1Team1.getName()),
            true
        );

        Team g_org1team1 = makeGetRequest(getTeamUri(org1Team1), Team.class, 200);
        Assertions.assertEquals(org1Team1, g_org1team1);

        Team g_org2team2 = makeGetRequest(getTeamUri(org2Team2), Team.class, 200);
        Assertions.assertEquals(org2Team2, g_org2team2);

        List<Team> org1Teams = getTeams(makeListRequest(getTeamsUri(org1.getName()), 200));
        Assertions.assertEquals(2, org1Teams.size());
        Assertions.assertTrue(org1Teams.contains(org1Team1));
        Assertions.assertTrue(org1Teams.contains(org1Team2));

        List<Team> org2Teams = getTeams(makeListRequest(getTeamsUri(org2.getName()), 200));
        Assertions.assertEquals(2, org2Teams.size());
        Assertions.assertTrue(org2Teams.contains(org2Team1));
        Assertions.assertTrue(org2Teams.contains(org2Team2));

        makeDeleteRequest(
            getOrgUri(org1),
            409,
            String.format("Can not delete Org(%s) as it has associated Team(s).", org1.getName()),
            true
        );

        makeDeleteRequest(getTeamUri(org1Team1), 200);
        org1Teams = getTeams(makeListRequest(getTeamsUri(org1.getName()), 200));
        Assertions.assertEquals(1, org1Teams.size());
        Assertions.assertTrue(org1Teams.contains(org1Team2));

        makeDeleteRequest(getTeamUri(org1Team2), 200);
        org1Teams = getTeams(makeListRequest(getTeamsUri(org1.getName()), 200));
        Assertions.assertEquals(0, org1Teams.size());

        makeDeleteRequest(getOrgUri(org1), 200);
        org2Teams = getTeams(makeListRequest(getTeamsUri(org2.getName()), 200));
        Assertions.assertEquals(2, org2Teams.size());
    }

    @Test
    public void testTeamInvalidOps() {
        // no org
        //TODO::Fix
        makeGetRequest(getTeamUri(org3Team1), 404, String.format("Org(%s) not found.", org3Team1.getOrg()), true);
        makeCreateRequest(
            getTeamsUri(org3Team1.getOrg()),
            org3Team1,
            404,
            String.format("Org(%s) not found.", org3Team1.getOrg()),
            true
        );
        makeListRequest(
            getTeamsUri(org3.getName()),
            404,
            String.format("Org(%s) not found.", org3Team1.getOrg()),
            true
        );
        makeDeleteRequest(getTeamUri(org3Team1), 404, String.format("Org(%s) not found.", org3Team1.getOrg()), true);

        //no  team
        makeGetRequest(getTeamUri(org1Team1), 404, String.format("Team(%s) not found.", org1Team1.getName()), true);
        makeDeleteRequest(getTeamUri(org1Team1), 404, String.format("Team(%s) not found.", org1Team1.getName()), true);
    }

}
