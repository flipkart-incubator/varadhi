package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.services.TeamService;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_ORG;
import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_TEAM;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TEAM_DELETE;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TEAM_GET;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.authenticated;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.hasBody;

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class TeamHandlers implements RouteProvider {
    private final TeamService teamService;

    public TeamHandlers(TeamService teamService) {
        this.teamService = teamService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
                "/v1/orgs/:org/teams",
                List.of(
                        new RouteDefinition(
                                HttpMethod.GET,
                                "",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::listTeams,
                                true,
                                Optional.of(PermissionAuthorization.of(TEAM_GET, "{org}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.GET,
                                "/:team/projects",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::listProjects,
                                true,
                                Optional.of(PermissionAuthorization.of(TEAM_GET, "{org}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.GET,
                                "/:team",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::get,
                                true,
                                Optional.of(PermissionAuthorization.of(TEAM_GET, "{org}/{team}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.POST,
                                "",
                                Set.of(hasBody),
                                new LinkedHashSet<>(),
                                this::create,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.DELETE,
                                "/:team", Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::delete,
                                true,
                                Optional.of(PermissionAuthorization.of(TEAM_DELETE, "{org}/{team}"))
                        )
                )
        ).get();
    }

    public void listTeams(RoutingContext ctx) {
        String orgName = ctx.pathParam(REQUEST_PATH_PARAM_ORG);
        List<Team> teams = teamService.getTeams(orgName);
        ctx.endApiWithResponse(teams);
    }

    public void listProjects(RoutingContext ctx) {
        String orgName = ctx.pathParam(REQUEST_PATH_PARAM_ORG);
        String teamName = ctx.pathParam(REQUEST_PATH_PARAM_TEAM);
        List<Project> projects = teamService.getProjects(teamName, orgName);
        ctx.endApiWithResponse(projects);
    }

    public void get(RoutingContext ctx) {
        String orgName = ctx.pathParam(REQUEST_PATH_PARAM_ORG);
        String teamName = ctx.pathParam(REQUEST_PATH_PARAM_TEAM);
        Team team = teamService.getTeam(teamName, orgName);
        ctx.endApiWithResponse(team);
    }

    public void create(RoutingContext ctx) {
        //TODO:: Authz check need to be explicit here.
        String orgName = ctx.pathParam(REQUEST_PATH_PARAM_ORG);
        Team team = ctx.body().asValidatedPojo(Team.class);
        if (!orgName.equals(team.getOrg())) {
            throw new IllegalArgumentException("Specified org name is different from org name in url");
        }

        Team createdTeam = teamService.createTeam(team);
        ctx.endApiWithResponse(createdTeam);
    }

    public void delete(RoutingContext ctx) {
        String orgName = ctx.pathParam(REQUEST_PATH_PARAM_ORG);
        String teamName = ctx.pathParam(REQUEST_PATH_PARAM_TEAM);
        teamService.deleteTeam(teamName, orgName);
        ctx.endApi();
    }

}
