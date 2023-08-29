package com.flipkart.varadhi.web.v1;

import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.services.TeamService;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import com.google.common.collect.Sets;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.flipkart.varadhi.Constants.*;
import static com.flipkart.varadhi.Constants.PathParams.ORG_PATH_PARAM;
import static com.flipkart.varadhi.Constants.PathParams.TEAM_PATH_PARAM;
import static com.flipkart.varadhi.auth.ResourceAction.TEAM_DELETE;
import static com.flipkart.varadhi.auth.ResourceAction.TEAM_GET;
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
                                HttpMethod.GET, "", Set.of(authenticated), Sets.newLinkedHashSet(), this::listTeams, true,
                                Optional.of(PermissionAuthorization.of(TEAM_GET, "{org}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.GET, "/:team/projects", Set.of(authenticated), Sets.newLinkedHashSet(), this::listProjects, true,
                                Optional.of(PermissionAuthorization.of(TEAM_GET, "{org}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.GET, "/:team", Set.of(authenticated), Sets.newLinkedHashSet(), this::get, true,
                                Optional.of(PermissionAuthorization.of(TEAM_GET, "{org}/{team}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.POST, "", Set.of(hasBody), Sets.newLinkedHashSet(), this::create, true,Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.DELETE, "/:team", Set.of(authenticated), Sets.newLinkedHashSet(), this::delete, true,
                                Optional.of(PermissionAuthorization.of(TEAM_DELETE, "{org}/{team}"))
                        )
                )
        ).get();
    }

    public void listTeams(RoutingContext ctx) {
        String orgName = ctx.pathParam(ORG_PATH_PARAM);
        List<Team> teams = this.teamService.getTeams(orgName);
        ctx.endRequestWithResponse(teams);
    }

    public void listProjects(RoutingContext ctx) {
        String orgName = ctx.pathParam(ORG_PATH_PARAM);
        String teamName = ctx.pathParam(TEAM_PATH_PARAM);
        List<Project> projects = this.teamService.getProjects(teamName, orgName);
        ctx.endRequestWithResponse(projects);
    }

    public void get(RoutingContext ctx) {
        String orgName = ctx.pathParam(ORG_PATH_PARAM);
        String teamName = ctx.pathParam(TEAM_PATH_PARAM);
        Team team = this.teamService.getTeam(teamName, orgName);
        ctx.endRequestWithResponse(team);
    }

    public void create(RoutingContext ctx) {
        //TODO:: Authz check need to be explicit here.
        String orgName = ctx.pathParam(ORG_PATH_PARAM);
        Team team = ctx.body().asPojo(Team.class);
        team.setVersion(INITIAL_VERSION);
        team.setOrgName(orgName);
        Team createdTeam = this.teamService.createTeam(team);
        ctx.endRequestWithResponse(createdTeam);
    }

    public void delete(RoutingContext ctx) {
        String orgName = ctx.pathParam(ORG_PATH_PARAM);
        String teamName = ctx.pathParam(TEAM_PATH_PARAM);
        //TODO::No topics and subscriptions for this project.
        this.teamService.deleteTeam(teamName, orgName);
        ctx.endRequest();
    }

}
