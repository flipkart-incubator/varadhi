package com.flipkart.varadhi.web.admin;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.services.TeamService;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.web.v1.admin.TeamHandlers;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.client.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class TeamHandlersTest extends WebTestBase {

    TeamHandlers teamHandlers;
    TeamService teamService;

    Org o1 = Org.of("OrgOne");


    @BeforeEach
    public void PreTest() throws InterruptedException {
        super.setUp();
        teamService = mock(TeamService.class);
        teamHandlers = new TeamHandlers(teamService);

        Route routeCreate = router.post("/orgs/:org/teams").handler(bodyHandler).handler(ctx -> {
            teamHandlers.setTeam(ctx);
            ctx.next();
        }).handler(wrapBlocking(teamHandlers::create));
        setupFailureHandler(routeCreate);
        Route routeGet = router.get("/orgs/:org/teams/:team").handler(wrapBlocking(teamHandlers::get));
        setupFailureHandler(routeGet);
        Route routeList = router.get("/orgs/:org/teams").handler(wrapBlocking(teamHandlers::list));
        setupFailureHandler(routeList);
        Route routeProjectList = router.get("/orgs/:org/teams/:team/projects")
                                       .handler(wrapBlocking(teamHandlers::listProjects));
        setupFailureHandler(routeProjectList);
        Route routeDelete = router.delete("/orgs/:org/teams/:team").handler(wrapBlocking(teamHandlers::delete));
        setupFailureHandler(routeDelete);
    }

    private String getTeamsUrl(String orgName) {
        return String.join("/", "/orgs", orgName, "teams");
    }

    private String getTeamUrl(Team team) {
        return String.join("/", getTeamsUrl(team.getOrg()), team.getName());
    }

    private String getProjectsUrl(Team team) {
        return String.join("/", getTeamUrl(team), "projects");
    }


    @AfterEach
    public void PostTest() throws InterruptedException {
        super.tearDown();
    }

    @Test
    public void testTeamCreate() throws InterruptedException {

        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, getTeamsUrl(o1.getName()));
        Team team1 = Team.of("team1", o1.getName());

        doReturn(team1).when(teamService).createTeam(eq(team1));
        Team team1Created = sendRequestWithEntity(request, team1, c(Team.class));
        Assertions.assertEquals(team1, team1Created);
        verify(teamService, times(1)).createTeam(eq(team1));

        String orgNotFoundError = String.format("Org(%s) not found.", team1.getOrg());
        doThrow(new ResourceNotFoundException(orgNotFoundError)).when(teamService).createTeam(team1);
        ErrorResponse response = sendRequestWithEntity(request, team1, 404, orgNotFoundError, c(ErrorResponse.class));
        Assertions.assertEquals(orgNotFoundError, response.reason());

        String duplicateOrgError = String.format(
            "Team(%s) already exists. Team is unique with in Org.",
            team1.getName()
        );
        doThrow(new DuplicateResourceException(duplicateOrgError)).when(teamService).createTeam(team1);
        response = sendRequestWithEntity(request, team1, 409, duplicateOrgError, c(ErrorResponse.class));
        Assertions.assertEquals(duplicateOrgError, response.reason());

        String someInternalError = "Some random error";
        doThrow(new MetaStoreException(someInternalError)).when(teamService).createTeam(team1);
        response = sendRequestWithEntity(request, team1, 500, someInternalError, c(ErrorResponse.class));
        Assertions.assertEquals(someInternalError, response.reason());
    }


    @Test
    public void testTeamGet() throws InterruptedException {
        Team team1 = Team.of("team1", o1.getName());

        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, getTeamUrl(team1));
        doReturn(team1).when(teamService).getTeam(team1.getName(), team1.getOrg());

        Team team1Get = sendRequestWithoutPayload(request, c(Team.class));
        Assertions.assertEquals(team1, team1Get);
        verify(teamService, times(1)).getTeam(team1.getName(), team1.getOrg());

        String notFoundError = String.format("Team(%s) not found.", team1.getName());
        doThrow(new ResourceNotFoundException(notFoundError)).when(teamService)
                                                             .getTeam(team1.getName(), team1.getOrg());
        sendRequestWithoutPayload(request, 404, notFoundError);
    }


    @Test
    public void testTeamList() throws Exception {
        List<Team> teamList = List.of(Team.of("team1", o1.getName()), Team.of("team2", o1.getName()));
        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, getTeamsUrl(o1.getName()));
        doReturn(teamList).when(teamService).getTeams(o1.getName());

        List<Team> teamListObtained = sendRequestWithoutPayload(request, t(new TypeReference<List<Team>>() {
        }));
        Assertions.assertEquals(teamList.size(), teamListObtained.size());
        Assertions.assertArrayEquals(teamList.toArray(), teamListObtained.toArray());
        verify(teamService, times(1)).getTeams(o1.getName());
    }

    @Test
    public void testProjectList() throws Exception {
        Team team1 = Team.of("team1", o1.getName());
        List<Project> projectList1 = List.of();
        List<Project> projectList2 = List.of(getProject("project1", team1), getProject("project2", team1));
        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, getProjectsUrl(team1));
        doReturn(projectList1).when(teamService).getProjects(team1.getName(), team1.getOrg());

        List<Project> projectList1Obtained = sendRequestWithoutPayload(request, t(new TypeReference<List<Project>>() {
        }));
        Assertions.assertEquals(projectList1.size(), projectList1Obtained.size());

        doReturn(projectList2).when(teamService).getProjects(team1.getName(), team1.getOrg());
        List<Project> projectList2Obtained = sendRequestWithoutPayload(request, t(new TypeReference<List<Project>>() {
        }));
        Assertions.assertArrayEquals(projectList2.toArray(), projectList2Obtained.toArray());
    }

    private Project getProject(String name, Team team) {
        return Project.of(name, "Some random value", team.getName(), team.getOrg());
    }

    @Test
    public void testTeamDelete() throws InterruptedException {
        Team team1 = Team.of("team1", o1.getName());

        HttpRequest<Buffer> request = createRequest(HttpMethod.DELETE, getTeamUrl(team1));
        doNothing().when(teamService).deleteTeam(team1.getName(), team1.getOrg());
        sendRequestWithoutPayload(request, null);
        verify(teamService, times(1)).deleteTeam(team1.getName(), team1.getOrg());

        String notFoundError = String.format("Team(%s) not found.", team1.getName());
        doThrow(new ResourceNotFoundException(notFoundError)).when(teamService)
                                                             .deleteTeam(team1.getName(), team1.getOrg());
        sendRequestWithoutPayload(request, 404, notFoundError);

        String invalidOpError = String.format(
            "Can not delete Team(%s) as it has associated Project(s).",
            team1.getName()
        );
        doThrow(new InvalidOperationForResourceException(invalidOpError)).when(teamService)
                                                                         .deleteTeam(team1.getName(), team1.getOrg());
        sendRequestWithoutPayload(request, 409, invalidOpError);
    }
}
