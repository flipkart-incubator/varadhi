package com.flipkart.varadhi.services;


import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.common.utils.JsonMapper;
import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.db.ZKMetaStore;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.entities.filters.OrgFilters;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        orgService = new OrgService(varadhiMetaStore.orgMetaStore(), varadhiMetaStore.teamMetaStore());
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
    public void testOrgFiltersOperations() throws JsonProcessingException {
        String orgName = "org1";
        // Create the organization
        Org org1 = Org.of(orgName);
        orgService.createOrg(org1);

        // Create OrgFilters using JSON (initially without filterA)
        String jsonCreate = """
            {
                "version": 0,
                "filters": {
                    "nameGroup.filterName": {
                        "op": "exists",
                        "key": "X_abc"
                    }
                }
            }
            """;
        OrgFilters orgFilters = JsonMapper.getMapper().readValue(jsonCreate, OrgFilters.class);
        orgService.createFilter(orgName, orgFilters);

        // Retrieve and verify that the created OrgFilters is not null
        OrgFilters retrievedFilters = orgService.getAllFilters(orgName);
        assertNotNull(retrievedFilters);

        // Update the filter by adding a new filter named "filterA" via a new JSON
        String jsonUpdate = """
            {
                "version": 0,
                "filters": {
                    "filterA": {
                        "op": "exists",
                        "key": "X_abc"
                    },
                    "nameGroup.filterName": {
                        "op": "exists",
                        "key": "X_abc"
                    }
                }
            }
            """;
        OrgFilters updatedFilters = JsonMapper.getMapper().readValue(jsonUpdate, OrgFilters.class);
        orgService.updateFilter(orgName, "filterA", updatedFilters);

        // Verify that "filterA" now exists
        assertTrue(orgService.filterExists(orgName, "filterA"));

        // Verify that a non-existent filter returns false
        assertFalse(orgService.filterExists(orgName, "nonExistent"));
    }

    @Test
    public void testDeleteFilter() throws JsonProcessingException {
        String orgName = "orgForDeleteFilter";
        // Create the organization
        Org org = Org.of(orgName);
        orgService.createOrg(org);

        // Create OrgFilters using JSON
        String json = """
            {
                "version": 0,
                "filters": {
                    "filterToDelete": {
                        "op": "exists",
                        "key": "X_abc"
                    }
                }
            }
            """;
        OrgFilters orgFilters = JsonMapper.getMapper().readValue(json, OrgFilters.class);
        orgService.createFilter(orgName, orgFilters);

        // Ensure the filter exists before deletion
        assertNotNull(orgService.getAllFilters(orgName));

        // Delete the filter
        orgService.deleteFilter(orgName);

        // After deletion, retrieval should throw a ResourceNotFoundException
        ResourceNotFoundException exception = assertThrows(
            ResourceNotFoundException.class,
            () -> orgService.getAllFilters(orgName)
        );
        assertEquals("Filters(Filters) not found.", exception.getMessage());
    }

    @Test
    public void testDeleteFilterForNonExistentOrg() {
        // Attempt to delete filters for a non-existent org name should throw an exception
        String nonExistentOrg = "nonExistentOrg";
        ResourceNotFoundException exception = assertThrows(
            ResourceNotFoundException.class,
            () -> orgService.deleteFilter(nonExistentOrg)
        );
        assertEquals("Filters(Filters) not found.", exception.getMessage());
    }


}
