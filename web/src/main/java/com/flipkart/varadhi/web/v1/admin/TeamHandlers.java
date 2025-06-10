package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.Hierarchies;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.core.TeamService;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.common.Constants.ContextKeys.REQUEST_BODY;
import static com.flipkart.varadhi.common.Constants.MethodNames.*;

import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_ORG;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_TEAM;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;

@Slf4j
@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class TeamHandlers implements RouteProvider {
    private static final String API_NAME = "TEAM";

    private final TeamService teamService;

    public TeamHandlers(TeamService teamService) {
        this.teamService = teamService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
            "/v1/orgs/:org/teams",
            List.of(
                RouteDefinition.get(LIST, API_NAME, "").authorize(TEAM_LIST).build(this::getHierarchies, this::list),
                RouteDefinition.get(LIST_PROJECTS, API_NAME, "/:team/projects")
                               .authorize(PROJECT_LIST)
                               .build(this::getHierarchies, this::listProjects),
                RouteDefinition.get(GET, API_NAME, "/:team").authorize(TEAM_GET).build(this::getHierarchies, this::get),
                RouteDefinition.post(CREATE, API_NAME, "")
                               .hasBody()
                               .bodyParser(this::setTeam)
                               .authorize(TEAM_CREATE)
                               .build(this::getHierarchies, this::create),
                RouteDefinition.delete(DELETE, API_NAME, "/:team")
                               .authorize(TEAM_DELETE)
                               .build(this::getHierarchies, this::delete)
            )
        ).get();
    }

    public void setTeam(RoutingContext ctx) {
        Team team = ctx.body().asValidatedPojo(Team.class);
        ctx.put(REQUEST_BODY, team);
    }

    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        if (hasBody) {
            Team team = ctx.get(REQUEST_BODY);
            return Map.of(ResourceType.TEAM, new Hierarchies.TeamHierarchy(team.getOrg(), team.getName()));
        }
        String org = ctx.request().getParam(PATH_PARAM_ORG);
        String team = ctx.request().getParam(PATH_PARAM_TEAM);
        if (team == null) {
            return Map.of(ResourceType.ORG, new Hierarchies.OrgHierarchy(org));
        }
        return Map.of(ResourceType.TEAM, new Hierarchies.TeamHierarchy(org, team));
    }


    public void list(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        List<Team> teams = teamService.getTeams(orgName);
        ctx.endApiWithResponse(teams);
    }

    public void listProjects(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        String teamName = ctx.pathParam(PATH_PARAM_TEAM);
        List<Project> projects = teamService.getProjects(teamName, orgName);
        ctx.endApiWithResponse(projects);
    }

    public void get(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        String teamName = ctx.pathParam(PATH_PARAM_TEAM);
        Team team = teamService.getTeam(teamName, orgName);
        ctx.endApiWithResponse(team);
    }

    public void create(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        Team team = ctx.get(REQUEST_BODY);
        if (!orgName.equals(team.getOrg())) {
            throw new IllegalArgumentException("Specified org name is different from org name in url");
        }

        Team createdTeam = teamService.createTeam(team);
        ctx.endApiWithResponse(createdTeam);
    }

    public void delete(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        String teamName = ctx.pathParam(PATH_PARAM_TEAM);
        teamService.deleteTeam(teamName, orgName);
        ctx.endApi();
    }

}
