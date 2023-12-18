package com.flipkart.varadhi.deployment;

import com.flipkart.varadhi.VerticleDeployer;
import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.config.ServerConfiguration;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.services.MessagingStackProvider;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class LeanDeploymentVerticleDeployer extends VerticleDeployer {
    public LeanDeploymentVerticleDeployer(
            String hostName, Vertx vertx, ServerConfiguration configuration,
            MessagingStackProvider messagingStackProvider, MetaStoreProvider metaStoreProvider,
            MeterRegistry meterRegistry
    ) {
        super(hostName, vertx, configuration, messagingStackProvider, metaStoreProvider, meterRegistry);
    }

    @Override
    public List<RouteDefinition> getRouteDefinitions() {
        return List.of();
    }

    @Override
    public void deployVerticle(
            Vertx vertx,
            ServerConfiguration configuration
    ) {
        validateLeanDeploymentConstraints(configuration.getRestOptions());
        super.deployVerticle(vertx, configuration);
    }

    private void validateLeanDeploymentConstraints(RestOptions restOptions) {
        String defaultOrg = restOptions.getDefaultOrg();
        String defaultTeam = restOptions.getDefaultTeam();
        String defaultProject = restOptions.getDefaultProject();

        if (validateOrgConstraints(defaultOrg, defaultTeam, defaultProject)) {
            // If org is created, then team and project will be created by default.
            return;
        }

        if (validateTeamConstraints(defaultOrg, defaultTeam, defaultProject)) {
            // If team is created, then project will be created by default.
            return;
        }

        validateProjectConstraints(defaultOrg, defaultTeam, defaultProject);


    }

    private Boolean validateOrgConstraints(
            String defaultOrg,
            String defaultTeam,
            String defaultProject) {

        List<Org> orgs = orgService.getOrgs();

        if (orgs.size() > 1) {
            throw new InvalidConfigException("Lean deployment can not be enabled as there are more than one orgs.");
        }

        if (orgs.size() == 1 && !defaultOrg.equals(orgs.get(0).getName())) {
            throw new InvalidConfigException(String.format(
                    "Lean deployment can not be enabled as org with %s name is present.",
                    orgs.get(0).getName()));
        }

        if (orgs.isEmpty()) {
            createDefaultOrgTeamProject(defaultOrg, defaultTeam, defaultProject);
            return true;
        }
        return false;
    }

    private void createDefaultOrgTeamProject(String defaultOrg, String defaultTeam, String defaultProject) {
        log.debug("Creating default org, team and project as no org is present.");

        orgService.createOrg(new Org(defaultOrg, 0));
        teamService.createTeam(new Team(defaultTeam, 0, defaultOrg));
        projectService.createProject(new Project(defaultProject, 0, "", defaultTeam, defaultOrg));

        log.debug("Created default org, team and project as no org is present.");
    }

    private void validateProjectConstraints(
            String defaultOrg,
            String defaultTeam,
            String defaultProject) {

        List<Project> projects = teamService.getProjects(defaultTeam, defaultOrg);

        if (projects.size() > 1) {
            throw new InvalidConfigException("Lean deployment can not be enabled as there are more than one projects.");
        }
        if (projects.size() == 1 && !defaultProject.equals(projects.get(0).getName())) {
            throw new InvalidConfigException(String.format(
                    "Lean deployment can not be enabled as project with %s name is present.",
                    projects.get(0).getName()));
        }
        if (projects.isEmpty()) {
            log.debug("Creating default project as no team is present.");
            projectService.createProject(new Project(defaultProject, 0, "", defaultTeam, defaultOrg));
            log.debug("Created default project as no team is present.");
        }
    }

    private Boolean validateTeamConstraints(
            String defaultOrg,
            String defaultTeam,
            String defaultProject
    ) {
        List<Team> teams = teamService.getTeams(defaultOrg);

        if (teams.size() > 1) {
            throw new InvalidConfigException("Lean deployment can not be enabled as there are more than one teams.");
        }
        if (teams.size() == 1 && !defaultTeam.equals(teams.get(0).getName())) {
            throw new InvalidConfigException(String.format(
                    "Lean deployment can not be enabled as team with %s name is present.",
                    teams.get(0).getName()));
        }
        if (teams.isEmpty()) {
            createTeamAndProject(defaultOrg, defaultTeam, defaultProject);
            return true;
        }

        return false;
    }

    private void createTeamAndProject(
            String defaultOrg,
            String defaultTeam,
            String defaultProject
    ) {
        log.debug("Creating default team and project as no team is present.");

        teamService.createTeam(new Team(defaultTeam, 0, defaultOrg));
        projectService.createProject(new Project(defaultProject, 0, "", defaultTeam, defaultOrg));

        log.debug("Created default team and project as no team is present.");
    }

}

