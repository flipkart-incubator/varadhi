package com.flipkart.varadhi.core;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.db.ZKMetaStore;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.spi.db.TopicStore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class ProjectServiceTest {

    TestingServer zkCuratorTestingServer;
    OrgService orgService;
    TeamService teamService;
    ProjectService projectService;
    CuratorFramework zkCurator;
    VaradhiMetaStore varadhiMetaStore;

    Org org1, org2;
    Team o1t1, o1t2, o2t1;
    Project o1t1p1, o1t1p2, o2t1p1;

    @BeforeEach
    void PreTest() throws Exception {
        zkCuratorTestingServer = new TestingServer();
        zkCurator = spy(
            CuratorFrameworkFactory.newClient(
                zkCuratorTestingServer.getConnectString(),
                new ExponentialBackoffRetry(1000, 1)
            )
        );
        zkCurator.start();
        varadhiMetaStore = spy(new VaradhiMetaStore(new ZKMetaStore(zkCurator)));
        orgService = new OrgService(varadhiMetaStore.orgs(), varadhiMetaStore.teams());
        teamService = new TeamService(varadhiMetaStore);
        projectService = spy(new ProjectService(varadhiMetaStore));
        org1 = Org.of("TestOrg1");
        org2 = Org.of("TestOrg2");
        o1t1 = Team.of("TestTeam1", org1.getName());
        o1t2 = Team.of("TestTeam2", org1.getName());
        o2t1 = Team.of("TestTeam1", org2.getName());
        o1t1p1 = Project.of("o1t1p1", "", o1t1.getName(), o1t1.getOrg());
        o1t1p2 = Project.of("o1t1p2", "", o1t1.getName(), o1t1.getOrg());
        o2t1p1 = Project.of("o2t1p1", "", o2t1.getName(), o2t1.getOrg());
        orgService.createOrg(org1);
        teamService.createTeam(o1t1);
    }

    @Test
    void testCreateProject() {
        Project o1t1p1Created = projectService.createProject(o1t1p1);
        Project o1t1p1Get = projectService.getProject(o1t1p1.getName());
        Assertions.assertEquals(o1t1p1, o1t1p1Created);
        Assertions.assertEquals(o1t1p1, o1t1p1Get);

        Project dummyP1 = Project.of("dummyP", "", o1t1.getName(), "dummyOrg");
        validateResourceNotFound(
            String.format(
                "Org(%s) not found. For Project creation, associated Org and Team should exist.",
                dummyP1.getOrg()
            ),
            () -> projectService.createProject(dummyP1)
        );


        Project dummyP2 = Project.of("dummyP", "", "dummyTeam", o1t1.getOrg());
        validateResourceNotFound(
            String.format(
                "Team(%s) not found. For Project creation, associated Org and Team should exist.",
                dummyP2.getTeam()
            ),
            () -> projectService.createProject(dummyP2)
        );


        validateDuplicateProject(o1t1p1, () -> projectService.createProject(o1t1p1));

        teamService.createTeam(o1t2);
        Project duplicateP1 = Project.of(o1t1p1.getName(), "", o1t2.getName(), o1t2.getOrg());
        validateDuplicateProject(duplicateP1, () -> projectService.createProject(duplicateP1));

        orgService.createOrg(org2);
        teamService.createTeam(o2t1);
        Project duplicateP2 = Project.of(o1t1p1.getName(), "", o2t1.getName(), o2t1.getOrg());
        validateDuplicateProject(duplicateP2, () -> projectService.createProject(duplicateP2));
    }

    private void validateDuplicateProject(Project project, MethodCaller caller) {
        String errorMsg = String.format("Project(%s) already exists.", project.getName());
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
    void testGetProject() {
        Project dummyP1 = Project.of("dummyP", "", o1t1.getName(), "dummyOrg");
        validateResourceNotFound(
            String.format("Project(%s) not found.", dummyP1.getName()),
            () -> projectService.getProject(dummyP1.getName())
        );
    }

    @Test
    void testUpdateProject() {
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
            conflictingUpdate,
            InvalidOperationForResourceException.class,
            () -> projectService.updateProject(o1t1p1)
        );

        o1t1p1.setVersion(o1t1p1.getVersion() + 10);
        o1t1p1.setTeam(o1t2.getName());
        validateException(
            conflictingUpdate,
            InvalidOperationForResourceException.class,
            () -> projectService.updateProject(o1t1p1)
        );

        Project pLatest = projectService.getProject(o1t1p1.getName());

        String argumentErr = String.format(
            "Project(%s) has same team name and description. Nothing to update.",
            pLatest.getName()
        );
        validateException(argumentErr, IllegalArgumentException.class, () -> projectService.updateProject(pLatest));

        orgService.createOrg(org2);
        Project orgUpdate = Project.of(o1t1p1.getName(), o1t1p1.getDescription(), o1t1p1.getTeam(), org2.getName());

        argumentErr = String.format("Project(%s) cannot be moved across organization.", orgUpdate.getName());
        validateException(argumentErr, IllegalArgumentException.class, () -> projectService.updateProject(orgUpdate));
    }

    @Test
    void testDeleteProject() {
        projectService.createProject(o1t1p1);
        projectService.createProject(o1t1p2);
        projectService.deleteProject(o1t1p2.getName());
        TopicStore topicStore = mock(TopicStore.class);
        when(varadhiMetaStore.topics()).thenReturn(topicStore);
        when(topicStore.getAllNames(o1t1p2.getName())).thenReturn(List.of("Dummy1"));
        InvalidOperationForResourceException e = Assertions.assertThrows(
            InvalidOperationForResourceException.class,
            () -> projectService.deleteProject(o1t1p2.getName())
        );

        Assertions.assertEquals(
            String.format("Cannot delete Project(%s), it has 1 associated topic(s).", o1t1p2.getName()),
            e.getMessage()
        );
    }

    interface MethodCaller {
        void call();
    }

}
