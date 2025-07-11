package com.flipkart.varadhi.core;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.db.ZKMetaStore;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.entities.filters.Condition;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.entities.filters.StringConditions;
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

import static org.junit.jupiter.api.Assertions.*;
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
        zkCurator = spy(
            CuratorFrameworkFactory.newClient(
                zkCuratorTestingServer.getConnectString(),
                new ExponentialBackoffRetry(1000, 1)
            )
        );
        zkCurator.start();
        VaradhiMetaStore varadhiMetaStore = new VaradhiMetaStore(new ZKMetaStore(zkCurator));
        orgService = new OrgService(varadhiMetaStore.orgs(), varadhiMetaStore.teams());
        teamService = new TeamService(varadhiMetaStore);
    }

    @Test
    public void testCreateOrg() {
        Org org = Org.of("name1");
        Org orgCreated = orgService.createOrg(org);
        Org orgGet = orgService.getOrg(org.getName());
        Assertions.assertEquals(org, orgCreated);
        Assertions.assertEquals(org, orgGet);
        DuplicateResourceException e = Assertions.assertThrows(
            DuplicateResourceException.class,
            () -> orgService.createOrg(org)
        );
        Assertions.assertEquals(String.format("Org(%s) already exists.", org.getName()), e.getMessage());
    }

    @Test
    public void testCreateOrgNodeExists() throws Exception {
        Org org = Org.of("name1");
        CreateBuilder builder = spy(zkCurator.create());
        doReturn(builder).when(zkCurator).create();
        doThrow(new KeeperException.NodeExistsException()).when(builder).forPath(any(), any());
        DuplicateResourceException e = Assertions.assertThrows(
            DuplicateResourceException.class,
            () -> orgService.createOrg(org)
        );
        Assertions.assertEquals(String.format("Org(%s) already exists.", org.getName()), e.getMessage());
    }

    @Test
    public void testCreateOrgCuratorThrows() throws Exception {
        Org org = Org.of("name1");
        CreateBuilder builder = spy(zkCurator.create());
        doReturn(builder).when(zkCurator).create();
        doThrow(new KeeperException.BadVersionException()).when(builder).forPath(any(), any());
        MetaStoreException e = Assertions.assertThrows(MetaStoreException.class, () -> orgService.createOrg(org));
    }

    @Test
    public void testDeleteOrg() {
        Org org = Org.of("name1");
        orgService.createOrg(org);
        orgService.deleteOrg(org.getName());
        ResourceNotFoundException e = Assertions.assertThrows(
            ResourceNotFoundException.class,
            () -> orgService.getOrg(org.getName())
        );
        Assertions.assertEquals(String.format("Org(%s) not found.", org.getName()), e.getMessage());
    }

    @Test
    public void testDeleteOrgHasTeams() {
        Org org1 = Org.of("Org_1");
        Org org2 = Org.of("Org_2");
        Team team1 = Team.of("team1", org1.getName());
        Team team2 = Team.of("team2", org1.getName());
        orgService.createOrg(org1);
        orgService.createOrg(org2);
        teamService.createTeam(team1);
        teamService.createTeam(team2);
        InvalidOperationForResourceException e = Assertions.assertThrows(
            InvalidOperationForResourceException.class,
            () -> orgService.deleteOrg(org1.getName())
        );
        Assertions.assertEquals(
            String.format("Can not delete Org(%s) as it has associated Team(s).", org1.getName()),
            e.getMessage()
        );
        teamService.deleteTeam(team1.getName(), team1.getOrg());
        teamService.deleteTeam(team2.getName(), team2.getOrg());
        orgService.deleteOrg(org1.getName());
    }

    @Test
    public void testListOrgs() {
        Org org = Org.of("name1");
        List<Org> orgListOriginal = orgService.getOrgs();
        orgService.createOrg(org);
        List<Org> orgListNew = orgService.getOrgs();
        Assertions.assertTrue(orgListNew.contains(org));
    }


    @Test
    public void testOrgFiltersOperations() {
        String orgName = "org1";
        // Create the organization
        Org org1 = Org.of(orgName);
        orgService.createOrg(org1);
        Map<String, Condition> conditionMap = new HashMap<>();
        conditionMap.put("filterA", new StringConditions.ExistsCondition("X_abc"));
        OrgFilters orgFilters = new OrgFilters(0, conditionMap);

        orgService.createFilter(orgName, orgFilters);

        // Retrieve and verify that the created OrgFilters is not null
        OrgFilters retrievedFilters = orgService.getAllFilters(orgName);
        assertNotNull(retrievedFilters);

        // Update the filter by adding a new filter named "filterA" via a new JSON
        Map<String, Condition> conditionMap2 = new HashMap<>();
        conditionMap2.put("filterA", new StringConditions.ExistsCondition("X_abc"));
        conditionMap2.put("nameGroup.filterName", new StringConditions.ExistsCondition("X_abc"));

        OrgFilters updatedFilters = new OrgFilters(0, conditionMap2);
        orgService.updateFilter(orgName, updatedFilters);

        // Verify that "filterA" now exists
        assertNotNull(orgService.getFilter(orgName, "filterA"));

        // Verify that a non-existent filter returns false
        assertNull(orgService.getFilter(orgName, "nonExistent"));
    }


}
