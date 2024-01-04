package com.flipkart.varadhi.deployment;

import com.flipkart.varadhi.config.ServerConfiguration;
import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.services.OrgService;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.services.TeamService;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.services.MessagingStackProvider;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import com.flipkart.varadhi.utils.YamlLoader;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Vertx;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class LeanDeploymentVerticleDeployerTest {

    TestingServer zkCuratorTestingServer;
    CuratorFramework zkCurator;

    MetaStore varadhiMetaStore;

    LeanDeploymentVerticleDeployer leanDeploymentVerticleDeployer;

    MessagingStackProvider messagingStackProvider;

    MetaStoreProvider metaStoreProvider;

    ServerConfiguration serverConfiguration;

    MeterRegistry meterRegistry;

    Vertx vertx = Vertx.vertx();

    private OrgService orgService;
    private TeamService teamService;
    private ProjectService projectService;


    private static final String TEST_ORG = "testOrg";
    private static final String TEST_TEAM = "testTeam";
    private static final String TEST_PROJECT = "testProject";

    private static final List<String> MANAGEABLE_ENTITIES_API_ENDPOINTS = List.of(
            "/v1/orgs",
            "/v1/orgs/:org/teams",
            "/v1/projects"
    );

    @BeforeEach
    public void setup() throws Exception {
        zkCuratorTestingServer = new TestingServer();
        zkCurator = spy(CuratorFrameworkFactory.newClient(
                zkCuratorTestingServer.getConnectString(), new ExponentialBackoffRetry(1000, 1)));
        zkCurator.start();
        varadhiMetaStore = new VaradhiMetaStore(zkCurator);

        messagingStackProvider = mock(MessagingStackProvider.class);
        metaStoreProvider = mock(MetaStoreProvider.class);
        meterRegistry = mock(MeterRegistry.class);

        when(metaStoreProvider.getMetaStore()).thenReturn(varadhiMetaStore);
        when(messagingStackProvider.getProducerFactory()).thenReturn(mock(ProducerFactory.class));

        serverConfiguration = YamlLoader.loadConfig(
                "src/test/resources/testConfiguration.yml",
                ServerConfiguration.class);

        orgService = new OrgService(varadhiMetaStore);
        teamService = new TeamService(varadhiMetaStore);
        projectService = new ProjectService(
                varadhiMetaStore,
                serverConfiguration.getRestOptions().getProjectCacheBuilderSpec(),
                meterRegistry);

        leanDeploymentVerticleDeployer = new LeanDeploymentVerticleDeployer(
                "testHostName",
                vertx,
                serverConfiguration,
                messagingStackProvider,
                metaStoreProvider,
                meterRegistry
        );
    }

    @Test
    public void testNoEntitiesPresent_Success() {
        leanDeploymentVerticleDeployer.deployVerticle(vertx, serverConfiguration);

        Org org = orgService.getOrg(
                serverConfiguration.getRestOptions().getDefaultOrg());
        assertNotNull(org);
        assertEquals(serverConfiguration.getRestOptions().getDefaultOrg(), org.getName());

        Team team = teamService.getTeam(
                org.getName(), serverConfiguration.getRestOptions().getDefaultTeam());
        assertNotNull(team);
        assertEquals(serverConfiguration.getRestOptions().getDefaultTeam(), team.getName());

        Project project = projectService.getProject(
                serverConfiguration.getRestOptions().getDefaultProject());
        assertNotNull(project);
        assertEquals(serverConfiguration.getRestOptions().getDefaultProject(), project.getName());
    }

    @Test
    public void testEntitiesPresentWithDefaultName_Success() {
        Org org = new Org(serverConfiguration.getRestOptions().getDefaultOrg(), 0);
        orgService.createOrg(org);
        Team team = new Team(serverConfiguration.getRestOptions().getDefaultTeam(), 0, org.getName());
        teamService.createTeam(team);
        Project project = new Project(serverConfiguration.getRestOptions().getDefaultProject(),
                0,
                "",
                team.getName(),
                org.getName()
        );
        projectService.createProject(project);

        leanDeploymentVerticleDeployer.deployVerticle(vertx, serverConfiguration);

        org = orgService.getOrg(serverConfiguration.getRestOptions().getDefaultOrg());
        assertNotNull(org);
        assertEquals(serverConfiguration.getRestOptions().getDefaultOrg(), org.getName());

        team = teamService.getTeam(
                org.getName(), serverConfiguration.getRestOptions().getDefaultTeam());
        assertNotNull(team);
        assertEquals(serverConfiguration.getRestOptions().getDefaultTeam(), team.getName());

        project = projectService.getProject(
                serverConfiguration.getRestOptions().getDefaultProject());
        assertNotNull(project);
        assertEquals(serverConfiguration.getRestOptions().getDefaultProject(), project.getName());
    }

    @Test
    public void testDifferentOrgPresent_Failure() {
        Org org = new Org(TEST_ORG, 0);
        orgService.createOrg(org);

        InvalidConfigException exception = assertThrows(InvalidConfigException.class,
                () -> leanDeploymentVerticleDeployer.deployVerticle(vertx, serverConfiguration));

        assertEquals(String.format(
                "Lean deployment can not be enabled as org with %s name is present.",
                TEST_ORG), exception.getMessage());
    }

    @Test
    public void testMultipleOrgsPresent_Failure() {
        Org org1 = new Org(TEST_ORG, 0);
        Org org2 = new Org(TEST_ORG + "2", 0);
        orgService.createOrg(org1);
        orgService.createOrg(org2);

        InvalidConfigException exception = assertThrows(InvalidConfigException.class,
                () -> leanDeploymentVerticleDeployer.deployVerticle(vertx, serverConfiguration));

        assertEquals("Lean deployment can not be enabled as there are more than one orgs.",
                exception.getMessage());
    }

    @Test
    public void testDifferentTeamPresent_Failure() {
        Org org = new Org(serverConfiguration.getRestOptions().getDefaultOrg(), 0);
        orgService.createOrg(org);
        Team team = new Team(TEST_TEAM, 0, org.getName());
        teamService.createTeam(team);

        InvalidConfigException exception = assertThrows(InvalidConfigException.class,
                () -> leanDeploymentVerticleDeployer.deployVerticle(vertx, serverConfiguration));

        assertEquals(String.format(
                "Lean deployment can not be enabled as team with %s name is present.",
                TEST_TEAM), exception.getMessage());
    }

    @Test
    public void testMultipleTeamsPresent_Failure() {
        Org org = new Org(serverConfiguration.getRestOptions().getDefaultOrg(), 0);
        orgService.createOrg(org);
        Team team1 = new Team(TEST_TEAM, 0, org.getName());
        Team team2 = new Team(TEST_TEAM + "2", 0, org.getName());
        teamService.createTeam(team1);
        teamService.createTeam(team2);

        InvalidConfigException exception = assertThrows(InvalidConfigException.class,
                () -> leanDeploymentVerticleDeployer.deployVerticle(vertx, serverConfiguration));

        assertEquals("Lean deployment can not be enabled as there are more than one teams.",
                exception.getMessage());
    }

    @Test
    public void testDifferentProjectPresent_Failure() {
        Org org = new Org(serverConfiguration.getRestOptions().getDefaultOrg(), 0);
        orgService.createOrg(org);
        Team team = new Team(serverConfiguration.getRestOptions().getDefaultTeam(), 0, org.getName());
        teamService.createTeam(team);
        Project project = new Project(TEST_PROJECT, 0, "", team.getName(), org.getName());
        projectService.createProject(project);

        InvalidConfigException exception = assertThrows(InvalidConfigException.class,
                () -> leanDeploymentVerticleDeployer.deployVerticle(vertx, serverConfiguration));

        assertEquals(String.format(
                "Lean deployment can not be enabled as project with %s name is present.",
                TEST_PROJECT), exception.getMessage());
    }

    @Test
    public void testMultipleProjectsPresent_Failure() {
        Org org = new Org(serverConfiguration.getRestOptions().getDefaultOrg(), 0);
        orgService.createOrg(org);
        Team team = new Team(serverConfiguration.getRestOptions().getDefaultTeam(), 0, org.getName());
        teamService.createTeam(team);
        Project project1 = new Project(TEST_PROJECT, 0, "", team.getName(), org.getName());
        Project project2 = new Project(TEST_PROJECT + "2", 0, "", team.getName(), org.getName());
        projectService.createProject(project1);
        projectService.createProject(project2);

        InvalidConfigException exception = assertThrows(InvalidConfigException.class,
                () -> leanDeploymentVerticleDeployer.deployVerticle(vertx, serverConfiguration));

        assertEquals("Lean deployment can not be enabled as there are more than one projects.",
                exception.getMessage());
    }

    @Test
    public void testManageableEntitiesHandlerNotPresent() {
        List<RouteDefinition> routeDefinitions = leanDeploymentVerticleDeployer.getRouteDefinitions();
        assertNotNull(routeDefinitions);
        assertFalse(isManageableEntitiesHandlerPresent(routeDefinitions));
    }

    private Boolean isManageableEntitiesHandlerPresent(List<RouteDefinition> routeDefinitions) {
        return routeDefinitions.stream()
                .anyMatch(routeDefinition ->
                        MANAGEABLE_ENTITIES_API_ENDPOINTS.stream()
                                .anyMatch(apiEndpoint -> apiEndpoint.equals(routeDefinition.path()))
                );
    }
}
