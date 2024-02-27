package com.flipkart.varadhi.services;

import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.entities.auth.IAMPolicyRecord;
import com.flipkart.varadhi.entities.auth.IAMPolicyRequest;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import io.micrometer.core.instrument.Clock;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.flipkart.varadhi.utils.AuthZHelper.getAuthResourceFQN;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AuthZServiceTest {
    TestingServer zkCuratorTestingServer;

    AuthZService authZService;

    OrgService orgService;
    TeamService teamService;
    ProjectService projectService;
    CuratorFramework zkCurator;
    VaradhiMetaStore varadhiMetaStore;

    Org org1;
    Org org2;
    Team org1team1;
    Team org2team1;
    Project proj1;
    Project proj2;


    @BeforeEach
    public void PreTest() throws Exception {
        zkCuratorTestingServer = new TestingServer();
        zkCurator = CuratorFrameworkFactory.newClient(
                zkCuratorTestingServer.getConnectString(), new ExponentialBackoffRetry(1000, 1));
        zkCurator.start();
        varadhiMetaStore = spy(new VaradhiMetaStore(zkCurator));
        orgService = new OrgService(varadhiMetaStore);
        teamService = new TeamService(varadhiMetaStore);
        projectService =
                new ProjectService(varadhiMetaStore, "", new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM));
        authZService = new AuthZService(varadhiMetaStore, varadhiMetaStore);
        org1 = new Org("org1", 0);
        org2 = new Org("org2", 0);
        org1team1 = new Team("team1", 0, org1.getName());
        org2team1 = new Team("team1", 0, org2.getName());
        proj1 = new Project("proj1", 0, "", org1team1.getName(), org1.getName());
        proj2 = new Project("proj2", 0, "", org2team1.getName(), org2.getName());
    }

    @Test
    void testCreateIAMPolicyRecord() {
        // node on org resource
        String resourceId = org1.getName();
        orgService.createOrg(org1);
        IAMPolicyRequest request = new IAMPolicyRequest("usr1", Set.of("role1", "role2"));
        IAMPolicyRecord expect = new IAMPolicyRecord(
                getAuthResourceFQN(ResourceType.ORG, resourceId),
                Map.of(request.getSubject(), request.getRoles()), 0
        );
        IAMPolicyRecord nodeCreated = authZService.setIAMPolicy(ResourceType.ORG, resourceId, request);
        verify(varadhiMetaStore, times(1)).createIAMPolicyRecord(any());
        assertEquals(expect, nodeCreated);

        // org not exist
        var ior = assertThrowsExactly(
                IllegalArgumentException.class,
                () -> authZService.setIAMPolicy(ResourceType.ORG, org2.getName(), request)
        );
        assertEquals(
                String.format("Invalid resource id(%s) for resource type(%s).", org2.getName(), ResourceType.ORG),
                ior.getMessage()
        );

        // node on team resource
        teamService.createTeam(org1team1);
        resourceId = org1.getName() + ":" + org1team1.getName();
        IAMPolicyRequest request2 = new IAMPolicyRequest("usr1", Set.of("role1", "role2"));
        expect = new IAMPolicyRecord(
                getAuthResourceFQN(ResourceType.TEAM, resourceId),
                Map.of(request2.getSubject(), request2.getRoles()), 0
        );
        nodeCreated = authZService.setIAMPolicy(ResourceType.TEAM, resourceId, request);
        assertEquals(expect, nodeCreated);

        // wrong team name
        ior = assertThrowsExactly(
                IllegalArgumentException.class,
                () -> authZService.setIAMPolicy(ResourceType.TEAM, org2team1.getName(), request2)
        );
        assertEquals(
                String.format("Invalid resource id(%s) for resource type(%s).", org2team1.getName(), ResourceType.TEAM),
                ior.getMessage()
        );

        // node on project resource
        projectService.createProject(proj1);
        resourceId = proj1.getName();
        IAMPolicyRequest request3 = new IAMPolicyRequest("usr1", Set.of("role1", "role2"));
        expect = new IAMPolicyRecord(
                getAuthResourceFQN(ResourceType.PROJECT, resourceId),
                Map.of(request3.getSubject(), request3.getRoles()), 0
        );
        nodeCreated = authZService.setIAMPolicy(ResourceType.PROJECT, resourceId, request3);
        assertEquals(expect, nodeCreated);

        // wrong project name
        ior = assertThrowsExactly(
                IllegalArgumentException.class,
                () -> authZService.setIAMPolicy(ResourceType.PROJECT, proj2.getName(), request3)
        );
        assertEquals(
                String.format("Invalid resource id(%s) for resource type(%s).", proj2.getName(), ResourceType.PROJECT),
                ior.getMessage()
        );
    }

    @Test
    void testGetIAMPolicyRecord() {
        orgService.createOrg(org1);
        String resourceId = org1.getName();
        IAMPolicyRequest request = new IAMPolicyRequest("usr1", Set.of("role1", "role2"));
        IAMPolicyRecord expect = new IAMPolicyRecord(
                getAuthResourceFQN(ResourceType.ORG, resourceId),
                Map.of(request.getSubject(), request.getRoles()), 0
        );
        authZService.setIAMPolicy(ResourceType.ORG, resourceId, request);
        IAMPolicyRecord get = authZService.getIAMPolicy(ResourceType.ORG, resourceId);
        assertEquals(expect, get);

        teamService.createTeam(org1team1);
        resourceId = org1.getName() + ":" + org1team1.getName();
        expect = new IAMPolicyRecord(
                getAuthResourceFQN(ResourceType.TEAM, resourceId),
                Map.of(request.getSubject(), request.getRoles()), 0
        );
        authZService.setIAMPolicy(ResourceType.TEAM, resourceId, request);
        get = authZService.getIAMPolicy(ResourceType.TEAM, resourceId);
        assertEquals(expect, get);

        // non existent
        assertThrowsExactly(
                ResourceNotFoundException.class,
                () -> authZService.getIAMPolicy(ResourceType.ORG, org2.getName())
        );
    }

    @Test
    void testGetAllNodes() {
        orgService.createOrg(org1);
        orgService.createOrg(org2);
        teamService.createTeam(org1team1);
        teamService.createTeam(org2team1);
        projectService.createProject(proj1);
        projectService.createProject(proj2);

        IAMPolicyRequest request = new IAMPolicyRequest("usr1", Set.of("role1"));
        List<IAMPolicyRecord> expected = List.of(
                new IAMPolicyRecord(
                        getAuthResourceFQN(ResourceType.ORG, org1.getName()),
                        Map.of(request.getSubject(), request.getRoles()), 0
                ),
                new IAMPolicyRecord(
                        getAuthResourceFQN(ResourceType.ORG, org2.getName()),
                        Map.of(request.getSubject(), request.getRoles()), 0
                ),
                new IAMPolicyRecord(
                        getAuthResourceFQN(ResourceType.TEAM, org1.getName() + ":" + org1team1.getName()),
                        Map.of(request.getSubject(), request.getRoles()), 0
                ),
                new IAMPolicyRecord(
                        getAuthResourceFQN(ResourceType.TEAM, org2.getName() + ":" + org2team1.getName()),
                        Map.of(request.getSubject(), request.getRoles()), 0
                ),
                new IAMPolicyRecord(
                        getAuthResourceFQN(ResourceType.PROJECT, proj1.getName()),
                        Map.of(request.getSubject(), request.getRoles()), 0
                ),
                new IAMPolicyRecord(
                        getAuthResourceFQN(ResourceType.PROJECT, proj2.getName()),
                        Map.of(request.getSubject(), request.getRoles()), 0
                )
        );

        authZService.setIAMPolicy(ResourceType.ORG, org1.getName(), request);
        authZService.setIAMPolicy(ResourceType.ORG, org2.getName(), request);
        authZService.setIAMPolicy(ResourceType.TEAM, org1.getName() + ":" + org1team1.getName(), request);
        authZService.setIAMPolicy(ResourceType.TEAM, org2.getName() + ":" + org2team1.getName(), request);
        authZService.setIAMPolicy(ResourceType.PROJECT, proj1.getName(), request);
        authZService.setIAMPolicy(ResourceType.PROJECT, proj2.getName(), request);

        List<IAMPolicyRecord> nodes = authZService.getAll();
        assertEquals(6, nodes.size());
        assertTrue(expected.containsAll(nodes));
    }

    @Test
    void testUpdateNode() {
        orgService.createOrg(org1);
        String resourceId = org1.getName();
        IAMPolicyRequest request = new IAMPolicyRequest("usr1", Set.of("role1", "role2"));
        authZService.setIAMPolicy(ResourceType.ORG, resourceId, request);

        // update node
        IAMPolicyRecord expect = new IAMPolicyRecord(
                getAuthResourceFQN(ResourceType.ORG, resourceId),
                Map.of("usr1", Set.of("role1")), 1
        );
        IAMPolicyRequest update = new IAMPolicyRequest("usr1", Set.of("role1"));
        IAMPolicyRecord updated = authZService.setIAMPolicy(ResourceType.ORG, resourceId, update);
        assertEquals(expect, updated);
    }

    @Test
    void testDeleteNode() {
        orgService.createOrg(org1);
        String resourceId = org1.getName();
        IAMPolicyRequest request = new IAMPolicyRequest("usr1", Set.of("role1", "role2"));
        authZService.setIAMPolicy(ResourceType.ORG, resourceId, request);
        assertNotNull(authZService.getIAMPolicy(ResourceType.ORG, org1.getName()));

        authZService.deleteIAMPolicy(ResourceType.ORG, org1.getName());
        assertThrowsExactly(
                ResourceNotFoundException.class,
                () -> authZService.getIAMPolicy(ResourceType.ORG, org1.getName())
        );

        // delete on non existing
        assertThrowsExactly(
                ResourceNotFoundException.class,
                () -> authZService.deleteIAMPolicy(ResourceType.ORG, org2.getName())
        );
    }

    @Test
    void testSetIAMPolicy() {
        // create and update new node
        orgService.createOrg(org1);
        IAMPolicyRecord org1NodeExpected =
                new IAMPolicyRecord(
                        getAuthResourceFQN(ResourceType.ORG, org1.getName()),
                        Map.of("user1", Set.of("role1", "role2")), 1
                );
        IAMPolicyRequest org1Upd =
                new IAMPolicyRequest("user1", Set.of("role1", "role2"));
        // since node is not created, should throw exception on get
        assertThrows(
                ResourceNotFoundException.class,
                () -> authZService.getIAMPolicy(ResourceType.ORG, org1.getName())
        );

        // now we update the role assignment
        IAMPolicyRecord gotNode = authZService.setIAMPolicy(ResourceType.ORG, org1.getName(), org1Upd);
        assertEquals(org1NodeExpected, gotNode);

        // now we should be able to get the node
        assertDoesNotThrow(() -> authZService.getIAMPolicy(ResourceType.ORG, org1.getName()));

        // update existing node
        org1NodeExpected.setRoleAssignment("user2", Set.of("role1", "role2"));
        org1NodeExpected.setVersion(2);
        IAMPolicyRequest org1Upd2 =
                new IAMPolicyRequest("user2", Set.of("role1", "role2"));
        gotNode = authZService.setIAMPolicy(ResourceType.ORG, org1.getName(), org1Upd2);
        assertEquals(org1NodeExpected, gotNode);

        // update existing subject
        org1NodeExpected.setRoleAssignment("user1", Set.of("role3"));
        org1NodeExpected.setVersion(3);
        IAMPolicyRequest org1Upd3 =
                new IAMPolicyRequest("user1", Set.of("role3"));
        gotNode = authZService.setIAMPolicy(ResourceType.ORG, org1.getName(), org1Upd3);
        assertEquals(org1NodeExpected, gotNode);

        // check delete subject
        org1NodeExpected.setRoleAssignment("user1", Set.of());
        org1NodeExpected.setVersion(4);
        IAMPolicyRequest org1Upd4 = new IAMPolicyRequest("user1", Set.of());
        gotNode = authZService.setIAMPolicy(ResourceType.ORG, org1.getName(), org1Upd4);
        assertEquals(org1NodeExpected, gotNode);

        // new node on team resource
        teamService.createTeam(org1team1);
        String resourceId = org1.getName() + ":" + org1team1.getName();
        IAMPolicyRecord org1team1NodeExpected =
                new IAMPolicyRecord(getAuthResourceFQN(ResourceType.TEAM, resourceId), Map.of(), 1);
        org1team1NodeExpected.setRoleAssignment("user1", Set.of("role1", "role2"));
        IAMPolicyRequest org1team1Upd =
                new IAMPolicyRequest("user1", Set.of("role1", "role2"));
        authZService.setIAMPolicy(ResourceType.TEAM, resourceId, org1team1Upd);
        gotNode = authZService.setIAMPolicy(ResourceType.TEAM, resourceId, org1team1Upd);
        assertEquals(org1team1NodeExpected, gotNode);

        // should not modify org node
        IAMPolicyRecord org1Node = authZService.getIAMPolicy(ResourceType.ORG, org1.getName());
        assertNotEquals(org1team1NodeExpected, org1Node);

        // try to update node with invalid resourceId
        IAMPolicyRequest invalidUpd =
                new IAMPolicyRequest("user1", Set.of("role1", "role2"));
        var ior = assertThrowsExactly(
                IllegalArgumentException.class,
                () -> authZService.setIAMPolicy(ResourceType.ORG, "invalid", invalidUpd)
        );
        assertEquals(
                String.format("Invalid resource id(%s) for resource type(%s).", "invalid", ResourceType.ORG),
                ior.getMessage()
        );

        IAMPolicyRequest incorrectUpdate =
                new IAMPolicyRequest("user1", Set.of("role1", "role2"));
        ior = assertThrowsExactly(
                IllegalArgumentException.class,
                () -> authZService.setIAMPolicy(ResourceType.TEAM, org1.getName(), incorrectUpdate)
        );
        assertEquals(
                String.format("Invalid resource id(%s) for resource type(%s).", org1.getName(), ResourceType.TEAM),
                ior.getMessage()
        );
    }
}
