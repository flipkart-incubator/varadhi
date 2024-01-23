package com.flipkart.varadhi.services;

import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.db.ZNode;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class OrgServiceTest {
    TestingServer zkCuratorTestingServer;
    OrgService orgService;
    TeamService teamService;
    CuratorFramework zkCurator;

    @BeforeEach
    public void PreTest() throws Exception {
        zkCuratorTestingServer = new TestingServer();
        zkCurator = spy(CuratorFrameworkFactory.newClient(
                zkCuratorTestingServer.getConnectString(), new ExponentialBackoffRetry(1000, 1)));
        zkCurator.start();
        VaradhiMetaStore varadhiMetaStore = new VaradhiMetaStore(zkCurator);
        orgService = new OrgService(varadhiMetaStore);
        teamService = new TeamService(varadhiMetaStore);
    }

    @Test
    public void testCreateOrg() {
        Org org = new Org("name1", 0);
        Org orgCreated = orgService.createOrg(org);
        Org orgGet = orgService.getOrg(org.getName());
        Assertions.assertEquals(org, orgCreated);
        Assertions.assertEquals(org, orgGet);
        DuplicateResourceException e =
                Assertions.assertThrows(DuplicateResourceException.class, () -> orgService.createOrg(org));
        Assertions.assertEquals(
                String.format("Org(%s) already exists.", org.getName()), e.getMessage());
    }

    @Test
    public void testCreateOrgNodeExists() throws Exception {
        Org org = new Org("name1", 0);
        CreateBuilder builder = spy(zkCurator.create());
        doReturn(builder).when(zkCurator).create();
        doThrow(new KeeperException.NodeExistsException()).when(builder).forPath(any(), any());
        DuplicateResourceException e =
                Assertions.assertThrows(DuplicateResourceException.class, () -> orgService.createOrg(org));
        Assertions.assertEquals(
                String.format("Org(%s) already exists.", org.getName()), e.getMessage());
    }

    @Test
    public void testCreateOrgCuratorThrows() throws Exception {
        Org org = new Org("name1", 0);
        CreateBuilder builder = spy(zkCurator.create());
        doReturn(builder).when(zkCurator).create();
        doThrow(new KeeperException.BadVersionException()).when(builder).forPath(any(), any());
        MetaStoreException e =
                Assertions.assertThrows(MetaStoreException.class, () -> orgService.createOrg(org));
        ZNode zNode = ZNode.OfOrg(org.getName());
        Assertions.assertEquals(
                String.format("Failed to create Org(%s) at %s.", zNode.getName(), zNode.getPath()), e.getMessage());
    }

    @Test
    public void testDeleteOrg() {
        Org org = new Org("name1", 0);
        orgService.createOrg(org);
        orgService.deleteOrg(org.getName());
        ResourceNotFoundException e =
                Assertions.assertThrows(ResourceNotFoundException.class, () -> orgService.getOrg(org.getName()));
        Assertions.assertEquals(String.format("Org(%s) not found.", org.getName()), e.getMessage());
    }

    @Test
    public void testDeleteOrgHasTeams() {
        Org org1 = new Org("Org_1", 0);
        Org org2 = new Org("Org_2", 0);
        Team team1 = new Team("team1", 0, org1.getName());
        Team team2 = new Team("team2", 0, org1.getName());
        orgService.createOrg(org1);
        orgService.createOrg(org2);
        teamService.createTeam(team1);
        teamService.createTeam(team2);
        InvalidOperationForResourceException e =
                Assertions.assertThrows(
                        InvalidOperationForResourceException.class, () -> orgService.deleteOrg(org1.getName()));
        Assertions.assertEquals(
                String.format("Can not delete Org(%s) as it has associated Team(s).", org1.getName()), e.getMessage());
        teamService.deleteTeam(team1.getName(), team1.getOrg());
        teamService.deleteTeam(team2.getName(), team2.getOrg());
        orgService.deleteOrg(org1.getName());
    }

    @Test
    public void testListOrgs() {
        Org org = new Org("name1", 0);
        List<Org> orgListOriginal = orgService.getOrgs();
        orgService.createOrg(org);
        List<Org> orgListNew = orgService.getOrgs();
        Assertions.assertTrue(orgListNew.contains(org));
    }


}
