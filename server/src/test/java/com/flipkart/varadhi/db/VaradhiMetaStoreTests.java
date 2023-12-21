package com.flipkart.varadhi.db;

/**
 * @author kaur.prabhpreet
 * On 22/12/23
 */

import com.flipkart.varadhi.entities.*;
import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class VaradhiMetaStoreTests {

    @Mock
    private ZKMetaStore zkMetaStore;
    @Mock
    private CuratorFramework zkCurator;
    @Mock
    private Org org;
    @Mock
    private Team team;
    @Mock
    private Project project;
    @Mock
    private VaradhiTopic varadhiTopic;

    private VaradhiMetaStore varadhiMetaStore;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        varadhiMetaStore = new VaradhiMetaStore(zkCurator);
        //varadhiMetaStore.zkMetaStore = zkMetaStore;
    }

    @Test
    void createOrgSuccessfully() {
        varadhiMetaStore.createOrg(org);
        verify(zkMetaStore, times(1)).createZNodeWithData(any(), eq(org));
    }

    @Test
    void getOrgSuccessfully() {
        varadhiMetaStore.getOrg("orgName");
        verify(zkMetaStore, times(1)).getZNodeDataAsPojo(any(), eq(Org.class));
    }

    @Test
    void checkOrgExistsSuccessfully() {
        varadhiMetaStore.checkOrgExists("orgName");
        verify(zkMetaStore, times(1)).zkPathExist(any());
    }

    @Test
    void deleteOrgSuccessfully() {
        varadhiMetaStore.deleteOrg("orgName");
        verify(zkMetaStore, times(1)).deleteZNode(any());
    }

    @Test
    void getOrgsSuccessfully() {
        varadhiMetaStore.getOrgs();
        verify(zkMetaStore, times(1)).listChildren(any());
    }

    @Test
    void getTeamNamesSuccessfully() {
        varadhiMetaStore.getTeamNames("orgName");
        verify(zkMetaStore, times(1)).listChildren(any());
    }

    @Test
    void getTeamsSuccessfully() {
        when(varadhiMetaStore.getTeamNames(anyString())).thenReturn(Arrays.asList("team1", "team2"));
        List<Team> teams = varadhiMetaStore.getTeams("orgName");
        assertEquals(2, teams.size());
    }

    @Test
    void createTeamSuccessfully() {
        varadhiMetaStore.createTeam(team);
        verify(zkMetaStore, times(1)).createZNodeWithData(any(), eq(team));
    }

    @Test
    void getTeamSuccessfully() {
        varadhiMetaStore.getTeam("teamName", "orgName");
        verify(zkMetaStore, times(1)).getZNodeDataAsPojo(any(), eq(Team.class));
    }

    @Test
    void checkTeamExistsSuccessfully() {
        varadhiMetaStore.checkTeamExists("teamName", "orgName");
        verify(zkMetaStore, times(1)).zkPathExist(any());
    }

    @Test
    void deleteTeamSuccessfully() {
        varadhiMetaStore.deleteTeam("teamName", "orgName");
        verify(zkMetaStore, times(1)).deleteZNode(any());
    }

    @Test
    void getProjectsSuccessfully() {
        varadhiMetaStore.getProjects("teamName", "orgName");
        verify(zkMetaStore, times(1)).listChildren(any());
    }

    @Test
    void createProjectSuccessfully() {
        varadhiMetaStore.createProject(project);
        verify(zkMetaStore, times(1)).createZNodeWithData(any(), eq(project));
    }

    @Test
    void getProjectSuccessfully() {
        varadhiMetaStore.getProject("projectName");
        verify(zkMetaStore, times(1)).getZNodeDataAsPojo(any(), eq(Project.class));
    }

    @Test
    void checkProjectExistsSuccessfully() {
        varadhiMetaStore.checkProjectExists("projectName");
        verify(zkMetaStore, times(1)).zkPathExist(any());
    }

    @Test
    void deleteProjectSuccessfully() {
        varadhiMetaStore.deleteProject("projectName");
        verify(zkMetaStore, times(1)).deleteZNode(any());
    }

    @Test
    void updateProjectSuccessfully() {
        varadhiMetaStore.updateProject(project);
        verify(zkMetaStore, times(1)).updateZNodeWithData(any(), eq(project));
    }

    @Test
    void getVaradhiTopicNamesSuccessfully() {
        varadhiMetaStore.getVaradhiTopicNames("projectName");
        verify(zkMetaStore, times(1)).listChildren(any());
    }

    @Test
    void listVaradhiTopicsSuccessfully() {
        varadhiMetaStore.listVaradhiTopics("projectName");
        verify(zkMetaStore, times(1)).listChildren(any());
    }

    @Test
    void createVaradhiTopicSuccessfully() {
        varadhiMetaStore.createVaradhiTopic(varadhiTopic);
        verify(zkMetaStore, times(1)).createZNodeWithData(any(), eq(varadhiTopic));
    }

    @Test
    void checkVaradhiTopicExistsSuccessfully() {
        varadhiMetaStore.checkVaradhiTopicExists("varadhiTopicName");
        verify(zkMetaStore, times(1)).getZNodeDataAsPojo(any(), eq(VaradhiTopic.class));
    }

    @Test
    void getVaradhiTopicSuccessfully() {
        varadhiMetaStore.getVaradhiTopic("varadhiTopicName");
        verify(zkMetaStore, times(1)).getZNodeDataAsPojo(any(), eq(VaradhiTopic.class));
    }

    @Test
    void deleteVaradhiTopicSuccessfully() {
        varadhiMetaStore.deleteVaradhiTopic("varadhiTopicName");
        verify(zkMetaStore, times(1)).deleteZNode(any());
    }
}
