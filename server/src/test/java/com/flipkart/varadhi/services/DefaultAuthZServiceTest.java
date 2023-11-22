package com.flipkart.varadhi.services;

import com.flipkart.varadhi.auth.RoleBindingNode;
import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;

public class DefaultAuthZServiceTest {
    TestingServer zkCuratorTestingServer;

    DefaultAuthZService defaultAuthZService;

    OrgService orgService;
    TeamService teamService;
    ProjectService projectService;
    CuratorFramework zkCurator;

    Org org1;
    Org org2;
    Team org1team1;
    Team org2team1;
    Project proj1;
    Project proj2;


    @BeforeEach
    public void PreTest() throws Exception {
        zkCuratorTestingServer = new TestingServer();
        zkCurator = spy(CuratorFrameworkFactory.newClient(
                zkCuratorTestingServer.getConnectString(), new ExponentialBackoffRetry(1000, 1)));
        zkCurator.start();
        VaradhiMetaStore varadhiMetaStore = new VaradhiMetaStore(zkCurator);
        orgService = new OrgService(varadhiMetaStore);
        teamService = new TeamService(varadhiMetaStore);
        projectService = new ProjectService(varadhiMetaStore);
        defaultAuthZService = new DefaultAuthZService(varadhiMetaStore);
        org1 = new Org("org1", 0);
        org2 = new Org("org2", 0);
        org1team1 = new Team("team1", 0, org1.getName());
        org2team1 = new Team("team1", 0, org2.getName());
        proj1 = new Project("proj1", 0, "", org1team1.getName(), org1.getName());
        proj2 = new Project("proj2", 0, "", org2team1.getName(), org2.getName());
    }

    @Test
    public void testCreateRoleBindingNode() {
        // node on org resource
        String resourceId = org1.getName();
        orgService.createOrg(org1);
        RoleBindingNode expect = new RoleBindingNode(resourceId, ResourceType.ORG, Map.of(), 0);
        RoleBindingNode nodeCreated = defaultAuthZService.createRoleBindingNode(resourceId, ResourceType.ORG);
        assertEquals(expect, nodeCreated);

        // org not exist
        var ior = assertThrowsExactly(
                InvalidOperationForResourceException.class,
                () -> defaultAuthZService.createRoleBindingNode(org2.getName(), ResourceType.ORG)
        );
        assertEquals(
                String.format("Invalid resource id(%s) for resource type(%s).", org2.getName(), ResourceType.ORG),
                ior.getMessage()
        );

        // node on team resource
        teamService.createTeam(org1team1);
        resourceId = org1.getName() + ":" + org1team1.getName();
        expect = new RoleBindingNode(resourceId, ResourceType.TEAM, Map.of(), 0);
        nodeCreated = defaultAuthZService.createRoleBindingNode(resourceId, ResourceType.TEAM);
        assertEquals(expect, nodeCreated);

        // wrong team name
        ior = assertThrowsExactly(
                InvalidOperationForResourceException.class,
                () -> defaultAuthZService.createRoleBindingNode(org2team1.getName(), ResourceType.TEAM)
        );
        assertEquals(
                String.format("Invalid resource id(%s) for resource type(%s).", org2team1.getName(), ResourceType.TEAM),
                ior.getMessage()
        );

        // node on project resource
        projectService.createProject(proj1);
        resourceId = proj1.getName();
        expect = new RoleBindingNode(resourceId, ResourceType.PROJECT, Map.of(), 0);
        nodeCreated = defaultAuthZService.createRoleBindingNode(resourceId, ResourceType.PROJECT);
        assertEquals(expect, nodeCreated);

        // wrong project name
        ior = assertThrowsExactly(
                InvalidOperationForResourceException.class,
                () -> defaultAuthZService.createRoleBindingNode(proj2.getName(), ResourceType.PROJECT)
        );
        assertEquals(
                String.format("Invalid resource id(%s) for resource type(%s).", proj2.getName(), ResourceType.PROJECT),
                ior.getMessage()
        );

        // duplicate resource
        DuplicateResourceException e = assertThrowsExactly(
                DuplicateResourceException.class,
                () -> defaultAuthZService.createRoleBindingNode(org1.getName(), ResourceType.ORG)
        );
        assertEquals(String.format("RoleBinding(%s) already exists.", org1.getName()), e.getMessage());

        // node on topic resource
        // TODO: implement for topic resource
    }

    @Test
    public void testGetRoleBindingNode() {
        orgService.createOrg(org1);
        String resourceId = org1.getName();
        RoleBindingNode expect = new RoleBindingNode(resourceId, ResourceType.ORG, Map.of(), 0);
        defaultAuthZService.createRoleBindingNode(resourceId, ResourceType.ORG);
        RoleBindingNode get = defaultAuthZService.getRoleBindingNode(resourceId);
        assertEquals(expect, get);

        teamService.createTeam(org1team1);
        resourceId = org1.getName() + ":" + org1team1.getName();
        expect = new RoleBindingNode(resourceId, ResourceType.TEAM, Map.of(), 0);
        defaultAuthZService.createRoleBindingNode(resourceId, ResourceType.TEAM);
        get = defaultAuthZService.getRoleBindingNode(resourceId);
        assertEquals(expect, get);

        // non existent
        var rne = assertThrowsExactly(
                ResourceNotFoundException.class,
                () -> defaultAuthZService.getRoleBindingNode(org2.getName())
        );
        assertEquals(String.format("RoleBinding(%s) not found.", org2.getName()), rne.getMessage());
    }

    @Test
    public void testGetAllNodes() {
        orgService.createOrg(org1);
        orgService.createOrg(org2);
        teamService.createTeam(org1team1);
        teamService.createTeam(org2team1);
        projectService.createProject(proj1);
        projectService.createProject(proj2);

        List<RoleBindingNode> expected = List.of(
                new RoleBindingNode(org1.getName(), ResourceType.ORG, Map.of(), 0),
                new RoleBindingNode(org2.getName(), ResourceType.ORG, Map.of(), 0),
                new RoleBindingNode(org1.getName() + ":" + org1team1.getName(), ResourceType.TEAM, Map.of(), 0),
                new RoleBindingNode(org2.getName() + ":" + org2team1.getName(), ResourceType.TEAM, Map.of(), 0),
                new RoleBindingNode(proj1.getName(), ResourceType.PROJECT, Map.of(), 0),
                new RoleBindingNode(proj2.getName(), ResourceType.PROJECT, Map.of(), 0)
        );

        defaultAuthZService.createRoleBindingNode(org1.getName(), ResourceType.ORG);
        defaultAuthZService.createRoleBindingNode(org2.getName(), ResourceType.ORG);
        defaultAuthZService.createRoleBindingNode(org1.getName() + ":" + org1team1.getName(), ResourceType.TEAM);
        defaultAuthZService.createRoleBindingNode(org2.getName() + ":" + org2team1.getName(), ResourceType.TEAM);
        defaultAuthZService.createRoleBindingNode(proj1.getName(), ResourceType.PROJECT);
        defaultAuthZService.createRoleBindingNode(proj2.getName(), ResourceType.PROJECT);

        List<RoleBindingNode> nodes = defaultAuthZService.getAllRoleBindingNodes();
        assertEquals(6, nodes.size());
        assertTrue(expected.containsAll(nodes));
    }

    @Test
    public void testUpdateNode() {
        orgService.createOrg(org1);
        String resourceId = org1.getName();
        RoleBindingNode expect = new RoleBindingNode(resourceId, ResourceType.ORG, Map.of(), 0);
        defaultAuthZService.createRoleBindingNode(resourceId, ResourceType.ORG);

        // update node but with wrong version
        expect.setRoleAssignment("user1", Set.of("role1", "role2"));
        expect.setVersion(1);
        var ior = assertThrowsExactly(
                InvalidOperationForResourceException.class,
                () -> defaultAuthZService.updateRoleBindingNode(expect)
        );
        assertEquals(String.format("Conflicting update, RoleBinding(%s) has been modified. Fetch latest and try again.",
                expect.getResourceId()
        ), ior.getMessage());

        // correct update
        expect.setRoleAssignment("user1", Set.of("role1", "role2"));
        expect.setVersion(0);
        RoleBindingNode updated = defaultAuthZService.updateRoleBindingNode(expect);
        assertEquals(expect, updated);

        // update non existing node
        RoleBindingNode node = new RoleBindingNode(org2.getName(), ResourceType.ORG, Map.of(), 0);
        var rne = assertThrowsExactly(
                ResourceNotFoundException.class,
                () -> defaultAuthZService.updateRoleBindingNode(node)
        );
        assertEquals(String.format("RoleBinding(%s) not found.", org2.getName()), rne.getMessage());
    }

    @Test
    public void testDeleteNode() {
        orgService.createOrg(org1);
        RoleBindingNode expect = new RoleBindingNode(org1.getName(), ResourceType.ORG, Map.of(), 0);
        defaultAuthZService.createRoleBindingNode(org1.getName(), ResourceType.ORG);
        RoleBindingNode got = defaultAuthZService.getRoleBindingNode(org1.getName());
        assertEquals(expect, got);

        defaultAuthZService.deleteRoleBindingNode(org1.getName());
        var rne = assertThrowsExactly(
                ResourceNotFoundException.class,
                () -> defaultAuthZService.getRoleBindingNode(org1.getName())
        );
        assertEquals(String.format("RoleBinding(%s) not found.", org1.getName()), rne.getMessage());
        assertDoesNotThrow(() -> defaultAuthZService.createRoleBindingNode(org1.getName(), ResourceType.ORG));

        // delete on non existing
        rne = assertThrowsExactly(ResourceNotFoundException.class,
                () -> defaultAuthZService.deleteRoleBindingNode(org2.getName())
        );
        assertEquals(String.format("RoleBinding on resource(%s) not found.", org2.getName()), rne.getMessage());
    }

    @Test
    public void testUpdateRoleAssignment() {
        // create and update new node
        orgService.createOrg(org1);
        RoleBindingNode org1NodeExpected =
                new RoleBindingNode(org1.getName(), ResourceType.ORG, Map.of("user1", Set.of("role1", "role2")), 1);
        RoleAssignmentUpdate org1Upd =
                new RoleAssignmentUpdate(org1.getName(), ResourceType.ORG, "user1", Set.of("role1", "role2"));
        // since node is not created, should throw exception on get
        assertThrows(ResourceNotFoundException.class, () -> defaultAuthZService.getRoleBindingNode(org1.getName()));

        // now we update the role assignment
        RoleBindingNode gotNode = defaultAuthZService.updateRoleAssignment(org1Upd);
        assertEquals(org1NodeExpected, gotNode);

        // now we should be able to get the node
        assertDoesNotThrow(() -> defaultAuthZService.getRoleBindingNode(org1.getName()));

        // update existing node
        org1NodeExpected.setRoleAssignment("user2", Set.of("role1", "role2"));
        org1NodeExpected.setVersion(2);
        RoleAssignmentUpdate org1Upd2 =
                new RoleAssignmentUpdate(org1.getName(), ResourceType.ORG, "user2", Set.of("role1", "role2"));
        gotNode = defaultAuthZService.updateRoleAssignment(org1Upd2);
        assertEquals(org1NodeExpected, gotNode);

        // update existing subject
        org1NodeExpected.setRoleAssignment("user1", Set.of("role3"));
        org1NodeExpected.setVersion(3);
        RoleAssignmentUpdate org1Upd3 =
                new RoleAssignmentUpdate(org1.getName(), ResourceType.ORG, "user1", Set.of("role3"));
        gotNode = defaultAuthZService.updateRoleAssignment(org1Upd3);
        assertEquals(org1NodeExpected, gotNode);

        // check delete subject
        org1NodeExpected.setRoleAssignment("user1", Set.of());
        org1NodeExpected.setVersion(4);
        RoleAssignmentUpdate org1Upd4 = new RoleAssignmentUpdate(org1.getName(), ResourceType.ORG, "user1", Set.of());
        gotNode = defaultAuthZService.updateRoleAssignment(org1Upd4);
        assertEquals(org1NodeExpected, gotNode);

        // new node on team resource
        teamService.createTeam(org1team1);
        String resourceId = org1.getName() + ":" + org1team1.getName();
        RoleBindingNode org1team1NodeExpected = new RoleBindingNode(resourceId, ResourceType.TEAM, Map.of(), 1);
        org1team1NodeExpected.setRoleAssignment("user1", Set.of("role1", "role2"));
        RoleAssignmentUpdate org1team1Upd =
                new RoleAssignmentUpdate(resourceId, ResourceType.TEAM, "user1", Set.of("role1", "role2"));
        defaultAuthZService.createRoleBindingNode(resourceId, ResourceType.TEAM);
        gotNode = defaultAuthZService.updateRoleAssignment(org1team1Upd);
        assertEquals(org1team1NodeExpected, gotNode);

        // should not modify org node
        RoleBindingNode org1Node = defaultAuthZService.getRoleBindingNode(org1.getName());
        assertNotEquals(org1team1NodeExpected, org1Node);

        // try to update node with invalid resourceId
        RoleAssignmentUpdate invalidUpd =
                new RoleAssignmentUpdate("invalid", ResourceType.ORG, "user1", Set.of("role1", "role2"));
        var ior = assertThrowsExactly(InvalidOperationForResourceException.class,
                () -> defaultAuthZService.updateRoleAssignment(invalidUpd)
        );
        assertEquals(
                String.format("Invalid resource id(%s) for resource type(%s).", "invalid", ResourceType.ORG),
                ior.getMessage()
        );

        RoleAssignmentUpdate incorrectUpdate =
                new RoleAssignmentUpdate(org1.getName(), ResourceType.TEAM, "user1", Set.of("role1", "role2"));
        ior = assertThrowsExactly(InvalidOperationForResourceException.class,
                () -> defaultAuthZService.updateRoleAssignment(incorrectUpdate)
        );
        assertEquals(
                String.format("Incorrect resource type(%s) for resource id(%s).", ResourceType.TEAM, org1.getName()),
                ior.getMessage()
        );
    }
}
