package com.flipkart.varadhi.services;

import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class ProjectServiceTest {

    TestingServer zkCuratorTestingServer;
    OrgService orgService;
    TeamService teamService;
    ProjectService projectService;
    CuratorFramework zkCurator;
    VaradhiMetaStore varadhiMetaStore;
    MeterRegistry meterRegistry;

    Org org1, org2;
    Team o1t1, o1t2, o2t1;
    Project o1t1p1, o1t1p2, o2t1p1;

    @BeforeEach
    public void PreTest() throws Exception {
        zkCuratorTestingServer = new TestingServer();
        zkCurator = spy(CuratorFrameworkFactory.newClient(
                zkCuratorTestingServer.getConnectString(), new ExponentialBackoffRetry(1000, 1)));
        zkCurator.start();
        varadhiMetaStore = spy(new VaradhiMetaStore(zkCurator));
        orgService = new OrgService(varadhiMetaStore);
        teamService = new TeamService(varadhiMetaStore);
        meterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        projectService = spy(new ProjectService(varadhiMetaStore, "", meterRegistry));
        org1 = new Org("TestOrg1", 0);
        org2 = new Org("TestOrg2", 0);
        o1t1 = new Team("TestTeam1", 0, org1.getName());
        o1t2 = new Team("TestTeam2", 0, org1.getName());
        o2t1 = new Team("TestTeam1", 0, org2.getName());
        o1t1p1 = new Project("o1t1p1", 0, "", o1t1.getName(), o1t1.getOrg());
        o1t1p2 = new Project("o1t1p2", 0, "", o1t1.getName(), o1t1.getOrg());
        o2t1p1 = new Project("o2t1p1", 0, "", o2t1.getName(), o2t1.getOrg());
        orgService.createOrg(org1);
        teamService.createTeam(o1t1);
    }

    @Test
    public void testCreateProject() {
        Project o1t1p1Created = projectService.createProject(o1t1p1);
        Project o1t1p1Get = projectService.getProject(o1t1p1.getName());
        Assertions.assertEquals(o1t1p1, o1t1p1Created);
        Assertions.assertEquals(o1t1p1, o1t1p1Get);

        Project dummyP1 = new Project("dummyP", 0, "", o1t1.getName(), "dummyOrg");
        validateResourceNotFound(String.format(
                "Org(%s) not found. For Project creation, associated Org and Team should exist.",
                dummyP1.getOrg()
        ), () -> projectService.createProject(dummyP1));


        Project dummyP2 = new Project("dummyP", 0, "", "dummyTeam", o1t1.getOrg());
        validateResourceNotFound(String.format(
                "Team(%s) not found. For Project creation, associated Org and Team should exist.",
                dummyP2.getTeam()
        ), () -> projectService.createProject(dummyP2));


        validateDuplicateProject(o1t1p1, () -> projectService.createProject(o1t1p1));

        teamService.createTeam(o1t2);
        Project duplicateP1 = new Project(o1t1p1.getName(), 0, "", o1t2.getName(), o1t2.getOrg());
        validateDuplicateProject(duplicateP1, () -> projectService.createProject(duplicateP1));

        orgService.createOrg(org2);
        teamService.createTeam(o2t1);
        Project duplicateP2 = new Project(o1t1p1.getName(), 0, "", o2t1.getName(), o2t1.getOrg());
        validateDuplicateProject(duplicateP2, () -> projectService.createProject(duplicateP2));
    }

    private void validateDuplicateProject(Project project, MethodCaller caller) {
        String errorMsg =
                String.format("Project(%s) already exists.", project.getName());
        validateException(errorMsg, DuplicateResourceException.class, caller);
    }

    private void validateResourceNotFound(String errorMsg, MethodCaller caller) {
        validateException(errorMsg, ResourceNotFoundException.class, caller);
    }

    private <T extends Exception> void validateException(String errorMsg, Class<T> clazz, MethodCaller caller) {
        T e = Assertions.assertThrows(clazz, caller::call);
        Assertions.assertEquals(errorMsg, e.getMessage());
    }


    @Test
    public void testGetProject() {
        Project dummyP1 = new Project("dummyP", 0, "", o1t1.getName(), "dummyOrg");
        validateResourceNotFound(
                String.format("Project(%s) not found.", dummyP1.getName()),
                () -> projectService.getProject(dummyP1.getName())
        );
    }

    @Test
    public void testUpdateProject() {
        projectService.createProject(o1t1p1);
        o1t1p1.setDescription("Some Description");
        int initialVersion = o1t1p1.getVersion();
        Project updatedProject = projectService.updateProject(o1t1p1);
        Assertions.assertTrue(updatedProject.getVersion() > initialVersion);
        Assertions.assertEquals(updatedProject, o1t1p1);

        o1t1p1.setTeam(o1t2.getName());
        updatedProject = projectService.updateProject(o1t1p1);
        Assertions.assertEquals(updatedProject, o1t1p1);

        o1t1p1.setTeam(o1t1.getName());
        o1t1p1.setDescription("Some Another Description");
        updatedProject = projectService.updateProject(o1t1p1);
        Assertions.assertEquals(updatedProject, o1t1p1);

        String conflictingUpdate = String.format(
                "Conflicting update, Project(%s) has been modified. Fetch latest and try again.",
                o1t1p1.getName()
        );
        o1t1p1.setVersion(o1t1p1.getVersion() - 1);
        o1t1p1.setTeam(o1t2.getName());
        validateException(
                conflictingUpdate, InvalidOperationForResourceException.class,
                () -> projectService.updateProject(o1t1p1)
        );

        o1t1p1.setVersion(o1t1p1.getVersion() + 10);
        o1t1p1.setTeam(o1t2.getName());
        validateException(
                conflictingUpdate, InvalidOperationForResourceException.class,
                () -> projectService.updateProject(o1t1p1)
        );

        Project pLatest = projectService.getProject(o1t1p1.getName());

        String argumentErr =
                String.format("Project(%s) has same team name and description. Nothing to update.", pLatest.getName());
        validateException(
                argumentErr, IllegalArgumentException.class,
                () -> projectService.updateProject(pLatest)
        );

        orgService.createOrg(org2);
        Project orgUpdate =
                new Project(o1t1p1.getName(), o1t1p1.getVersion(), o1t1p1.getDescription(), o1t1p1.getTeam(),
                        org2.getName()
                );

        argumentErr = String.format("Project(%s) can not be moved across organisation.", orgUpdate.getName());
        validateException(
                argumentErr, IllegalArgumentException.class,
                () -> projectService.updateProject(orgUpdate)
        );
    }

    @Test
    public void testDeleteProject() {
        projectService.createProject(o1t1p1);
        projectService.createProject(o1t1p2);
        projectService.deleteProject(o1t1p2.getName());

        doReturn(List.of("Dummy1")).when(varadhiMetaStore).getVaradhiTopicNames(o1t1p2.getName());
        InvalidOperationForResourceException e = Assertions.assertThrows(
                InvalidOperationForResourceException.class,
                () -> projectService.deleteProject(o1t1p2.getName())
        );

        Assertions.assertEquals(
                String.format("Can not delete Project(%s), it has associated entities.", o1t1p2.getName()),
                e.getMessage()
        );
    }

    @Test
    public void testGetCachedProject() {
        Counter getCounter = meterRegistry.counter("varadhi.cache.project.gets");
        Counter loadCounter = meterRegistry.counter("varadhi.cache.project.loads");
        projectService.createProject(o1t1p1);
        projectService.createProject(o1t1p2);
        for (int i = 0; i < 100; i++) {
            projectService.getCachedProject(o1t1p1.getName());
            projectService.getCachedProject(o1t1p2.getName());
        }
        Assertions.assertEquals(200, (int) getCounter.count());
        Assertions.assertEquals(2, (int) loadCounter.count());
    }

    interface MethodCaller {
        void call();
    }

}
