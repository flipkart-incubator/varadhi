package com.flipkart.varadhi.services;

import java.util.List;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.db.ZKMetaStore;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import io.micrometer.core.instrument.Clock;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.spy;

public class TeamServiceTest {
    TestingServer zkCuratorTestingServer;
    OrgService orgService;
    TeamService teamService;
    ProjectService projectService;
    CuratorFramework zkCurator;
    Org org1;
    Org org2;
    Team org1Team1;
    Team org2Team1;
    Team teamDummy;

    @BeforeEach
    public void PreTest() throws Exception {
        zkCuratorTestingServer = new TestingServer();
        zkCurator = spy(
            CuratorFrameworkFactory.newClient(
                zkCuratorTestingServer.getConnectString(),
                new ExponentialBackoffRetry(1000, 1)
            )
        );
        zkCurator.start();
        VaradhiMetaStore varadhiMetaStore = new VaradhiMetaStore(new ZKMetaStore(zkCurator));
        orgService = new OrgService(varadhiMetaStore.orgOperations(), varadhiMetaStore.teamOperations());
        teamService = new TeamService(varadhiMetaStore);
        projectService = new ProjectService(
            varadhiMetaStore,
            "",
            new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM)
        );
        org1 = Org.of("TestOrg1");
        org2 = Org.of("TestOrg2");
        org1Team1 = Team.of("TestTeam1", org1.getName());
        org2Team1 = Team.of("TestTeam1", org2.getName());
        teamDummy = Team.of("TeamDummy", "DummyOrg");
        orgService.createOrg(org1);
    }

    @Test
    public void testCreateTeam() {
        Team teamCreated = teamService.createTeam(org1Team1);
        Team teamGet = teamService.getTeam(org1Team1.getName(), org1Team1.getOrg());
        Assertions.assertEquals(org1Team1, teamCreated);
        Assertions.assertEquals(org1Team1, teamGet);

        DuplicateResourceException e = Assertions.assertThrows(
            DuplicateResourceException.class,
            () -> teamService.createTeam(org1Team1)
        );
        Assertions.assertEquals(String.format("Team(%s) already exists.", org1Team1.getName()), e.getMessage());
        validateOrgNotFound(teamDummy, () -> teamService.createTeam(teamDummy));
    }


    @Test
    public void testGetTeam() {
        teamService.createTeam(org1Team1);
        Team teamGet = teamService.getTeam(org1Team1.getName(), org1Team1.getOrg());
        Assertions.assertEquals(org1Team1, teamGet);

        validateOrgNotFound(teamDummy, () -> teamService.getTeam(org1Team1.getName(), teamDummy.getOrg()));

        validateOrgNotFound(teamDummy, () -> teamService.getTeam(teamDummy.getName(), teamDummy.getOrg()));
    }

    @Test
    public void testListTeam() {
        orgService.createOrg(org2);
        teamService.createTeam(org1Team1);
        teamService.createTeam(org2Team1);
        List<Team> org1Teams = teamService.getTeams(org1Team1.getOrg());
        Assertions.assertEquals(1, org1Teams.size());
        Assertions.assertEquals(org1Team1, org1Teams.get(0));
        List<Team> org2Teams = teamService.getTeams(org2Team1.getOrg());
        Assertions.assertEquals(1, org2Teams.size());
        Assertions.assertEquals(org2Team1, org2Teams.get(0));

        validateOrgNotFound(teamDummy, () -> teamService.getTeams(teamDummy.getOrg()));
    }

    @Test
    public void testDeleteTeam() {
        teamService.createTeam(org1Team1);
        teamService.deleteTeam(org1Team1.getName(), org1Team1.getOrg());
        validateOrgNotFound(teamDummy, () -> teamService.deleteTeam(teamDummy.getName(), teamDummy.getOrg()));

        orgService.createOrg(org2);
        teamService.createTeam(org2Team1);
        Project p1 = Project.of("Project1", "", org2Team1.getName(), org2Team1.getOrg());
        projectService.createProject(p1);
        InvalidOperationForResourceException eOp = Assertions.assertThrows(
            InvalidOperationForResourceException.class,
            () -> teamService.deleteTeam(org2Team1.getName(), org2Team1.getOrg())
        );
        Assertions.assertEquals(
            String.format("Can not delete Team(%s) as it has associated Project(s).", org2Team1.getName()),
            eOp.getMessage()
        );
    }

    @Test
    public void testGetProjects() {
        Project p1 = Project.of("Project1", "", org1Team1.getName(), org1Team1.getOrg());
        teamService.createTeam(org1Team1);
        projectService.createProject(p1);

        List<Project> projects = teamService.getProjects(org1Team1.getName(), org1Team1.getOrg());
        Assertions.assertEquals(1, projects.size());
        Assertions.assertEquals(p1, projects.get(0));

        validateOrgNotFound(teamDummy, () -> teamService.getProjects(teamDummy.getName(), teamDummy.getOrg()));

        teamDummy.setOrg(org1Team1.getOrg());
        ResourceNotFoundException eResource = Assertions.assertThrows(
            ResourceNotFoundException.class,
            () -> teamService.getProjects(teamDummy.getName(), teamDummy.getOrg())
        );
        Assertions.assertEquals(
            String.format("Team(%s) does not exists in the Org(%s).", teamDummy.getName(), teamDummy.getOrg()),
            eResource.getMessage()
        );
    }

    private void validateOrgNotFound(Team team, MethodCaller caller) {
        ResourceNotFoundException eResource = Assertions.assertThrows(
            ResourceNotFoundException.class,
            () -> caller.call()
        );
        Assertions.assertEquals(String.format("Org(%s) not found.", team.getOrg()), eResource.getMessage());
    }

    interface MethodCaller {
        void call();
    }
}
