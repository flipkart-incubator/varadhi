package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ProjectTests extends E2EBase {
    Org org1, org2, org3;
    Team org1Team1, org1Team2, org2Team1, org3Team1;

    Project o1t1Project1, o1t1Project2, o1t2Project3, o1t2Project1, o2t1Project1, o2t1Project4, o3t1Project5;

    @BeforeEach
    public void testSetup() {
        org1 = Org.of("org1");
        org2 = Org.of("org2");
        org3 = Org.of("org3");
        org1Team1 = Team.of("team1", org1.getName());
        org1Team2 = Team.of("team2", org1.getName());
        org2Team1 = Team.of("team1", org2.getName());
        org3Team1 = Team.of("team1", org3.getName());
        o1t1Project1 = Project.of("project1", "description1", org1Team1.getName(), org1Team1.getOrg());
        o1t1Project2 = Project.of("project2", "description1", org1Team1.getName(), org1Team1.getOrg());
        o1t2Project3 = Project.of("project3", "description1", org1Team2.getName(), org1Team2.getOrg());
        o1t2Project1 = Project.of("project1", "description1", org1Team2.getName(), org1Team2.getOrg());
        o2t1Project1 = Project.of("project1", "description1", org2Team1.getName(), org2Team1.getOrg());
        o2t1Project4 = Project.of("project4", "description1", org2Team1.getName(), org2Team1.getOrg());
        o3t1Project5 = Project.of("project5", "description1", org3Team1.getName(), org3Team1.getOrg());

        makeCreateRequest(getOrgsUri(), org1, 200);
        makeCreateRequest(getOrgsUri(), org2, 200);
        makeCreateRequest(getTeamsUri(org1Team1.getOrg()), org1Team1, 200);
        makeCreateRequest(getTeamsUri(org1Team2.getOrg()), org1Team2, 200);
        makeCreateRequest(getTeamsUri(org2Team1.getOrg()), org2Team1, 200);
    }

    @AfterEach
    public void testCleanup() {
        cleanupOrgs(List.of(org1, org2, org3));
    }


    @Test
    public void testProjectCRUD() {
        Project c_o1t1Project1 = makeCreateRequest(getProjectCreateUri(), o1t1Project1, 200);
        Project c_o1t1Project2 = makeCreateRequest(getProjectCreateUri(), o1t1Project2, 200);
        Project c_o1t2Project3 = makeCreateRequest(getProjectCreateUri(), o1t2Project3, 200);
        Project c_o2t1Project4 = makeCreateRequest(getProjectCreateUri(), o2t1Project4, 200);
        Assertions.assertEquals(o1t1Project1, c_o1t1Project1);
        Assertions.assertEquals(o1t1Project2, c_o1t1Project2);
        Assertions.assertEquals(o1t2Project3, c_o1t2Project3);
        Assertions.assertEquals(o2t1Project4, c_o2t1Project4);

        makeCreateRequest(
                getProjectCreateUri(), o1t1Project1, 409,
                String.format("Project(%s) already exists.", o1t1Project1.getName()),
                true
        );
        makeCreateRequest(
                getProjectCreateUri(), o1t2Project1, 409,
                String.format("Project(%s) already exists.", o1t2Project1.getName()),
                true
        );
        makeCreateRequest(
                getProjectCreateUri(), o2t1Project1, 409,
                String.format("Project(%s) already exists.", o2t1Project1.getName()),
                true
        );

        Project g_o1t1Project1 = makeGetRequest(getProjectUri(o1t1Project1), Project.class, 200);
        Assertions.assertEquals(o1t1Project1, g_o1t1Project1);
        Project g_o1t2Project3 = makeGetRequest(getProjectUri(o1t2Project3), Project.class, 200);
        Assertions.assertEquals(o1t2Project3, g_o1t2Project3);
        Project g_o2t1Project4 = makeGetRequest(getProjectUri(o2t1Project4), Project.class, 200);
        Assertions.assertEquals(o2t1Project4, g_o2t1Project4);

        makeGetRequest(
                getProjectUri(o3t1Project5), 404, String.format("Project(%s) not found.", o3t1Project5.getName()),
                true
        );


        validateTeamProjects(org1Team1, List.of(o1t1Project1, o1t1Project2));
        validateTeamProjects(org1Team2, List.of(o1t2Project3));
        validateTeamProjects(org2Team1, List.of(o2t1Project4));

        makeDeleteRequest(
                getTeamUri(org1Team1), 409,
                String.format("Can not delete Team(%s) as it has associated Project(s).", org1Team1.getName()), true
        );
        makeDeleteRequest(getProjectUri(o1t1Project2), 200);

        validateTeamProjects(org1Team1, List.of(o1t1Project1));

        // org update -- fail
        Project t1_o1t1Project1 =
                Project.of(o1t1Project1.getName(), o1t1Project1.getDescription(),
                        org2Team1.getName(),
                        org2Team1.getOrg()
                );
        makeUpdateRequest(
                getProjectCreateUri(), t1_o1t1Project1, 400,
                String.format("Project(%s) can not be moved across organisation.", t1_o1t1Project1.getName()), true
        );

        // no update -- fail
        Project t2_o1t1Project1 =
                Project.of(o1t1Project1.getName(), o1t1Project1.getDescription(),
                        o1t1Project1.getTeam(),
                        o1t1Project1.getOrg()
                );
        makeUpdateRequest(
                getProjectCreateUri(), t2_o1t1Project1, 400,
                String.format(
                        "Project(%s) has same team name and description. Nothing to update.",
                        t2_o1t1Project1.getName()
                ), true
        );

        // version mismatch -- fail
        Project t3_o1t1Project1 =
                Project.of(o1t1Project1.getName(), o1t1Project1.getDescription(),
                        org1Team2.getName(),
                        o1t1Project1.getOrg()
                );
        t3_o1t1Project1.setVersion(o1t1Project1.getVersion() + 1);
        makeUpdateRequest(getProjectCreateUri(), t3_o1t1Project1, 409, null, true);

        // name update  -- fail
        Project t4_o1t1Project1 = Project.of(o1t1Project1.getName() + "mismatch",
                o1t1Project1.getDescription(),
                o1t1Project1.getTeam(),
                o1t1Project1.getOrg()
        );
        makeUpdateRequest(
                getProjectCreateUri(), t4_o1t1Project1, 404,
                String.format("Project(%s) not found.", t4_o1t1Project1.getName()), true
        );

        // only team update -- fine.
        o1t1Project1.setTeam(org1Team2.getName());
        o1t1Project1 = makeUpdateRequest(getProjectCreateUri(), o1t1Project1, 200);


        validateTeamProjects(org1Team1, List.of());
        validateTeamProjects(org1Team2, List.of(o1t1Project1, o1t2Project3));

        makeDeleteRequest(getTeamUri(org1Team1), 200);
    }

    private void validateTeamProjects(Team team, List<Project> projects) {
        List<Project> teamProjects =
                getProjects(makeListRequest(getProjectListUri(team.getOrg(), team.getName()), 200));
        Assertions.assertEquals(projects.size(), teamProjects.size());
        projects.forEach(p -> Assertions.assertTrue(teamProjects.contains(p)));
    }


    @Test
    public void testProjectInvalidOps() {
        Project pCreated = makeCreateRequest(getProjectCreateUri(), o1t1Project1, 200);
        Project p1 = Project.of("prj1", "", "team_1", "org_1");
        makeGetRequest(getProjectUri(p1), 404, String.format("Project(%s) not found.", p1.getName()), true);
        makeDeleteRequest(getProjectUri(p1), 404, String.format("Project(%s) not found.", p1.getName()), true);

        p1 = Project.of("prj1", "", pCreated.getTeam(), "org_1");
        makeCreateRequest(
                getProjectCreateUri(), p1, 404,
                String.format(
                        "Org(%s) not found. For Project creation, associated Org and Team should exist.",
                        p1.getOrg()
                ), true
        );

        p1 = Project.of("prj1", "", "team_1", pCreated.getOrg());
        makeCreateRequest(
                getProjectCreateUri(), p1, 404,
                String.format(
                        "Team(%s) not found. For Project creation, associated Org and Team should exist.",
                        p1.getTeam()
                ), true
        );

        makeListRequest(
                getProjectListUri(o1t1Project1.getOrg(), p1.getTeam()), 404,
                String.format("Team(%s) does not exists in the Org(%s).", p1.getTeam(), o1t1Project1.getOrg()),
                true
        );
        makeListRequest(
                getProjectListUri("org_1", o1t1Project1.getTeam()), 404, String.format("Org(%s) not found.", "org_1"),
                true
        );

    }
}
