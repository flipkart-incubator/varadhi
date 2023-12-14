package com.flipkart.varadhi.deployment;

import com.flipkart.varadhi.VerticleDeployer;
import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.config.ServerConfiguration;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.exceptions.VaradhiException;
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
        validateLeanDeploymentConstraints(configuration.getRestOptions());
    }

    @Override
    public List<RouteDefinition> getRouteDefinitions() {
        return List.of();
    }

    private void validateLeanDeploymentConstraints(RestOptions restOptions) {
        String defaultOrg = restOptions.getDefaultOrg();
        String defaultTeam = restOptions.getDefaultTeam();
        String defaultProject = restOptions.getDefaultProject();

        List<Org> orgs = orgService.getOrgs();

        if (orgs.size() > 1) {
            throw new VaradhiException("Lean deployment can not be enabled as there are more than one orgs.");
        }

        if (orgs.size() == 1 && !defaultOrg.equals(orgs.get(0).getName())) {
            throw new VaradhiException("Lean deployment can not be enabled as org with different name is present. " +
                    "Please delete the org or change the default org.");
        }

        if (orgs.isEmpty()) {
            log.debug("Creating default org, team and project as no org is present.");

            orgService.createOrg(new Org(defaultOrg, 0));
            teamService.createTeam(new Team(defaultTeam, 0, defaultOrg));
            projectService.createProject(new Project(defaultProject, 0, "", defaultTeam, defaultOrg));

            log.debug("Created default org, team and project as no org is present.");
            return;
        }

        if (isDefaultTeamPresent(defaultTeam, defaultOrg)) {
            log.debug("validate default project");
            validateProjectConstraints(defaultProject, defaultTeam, defaultOrg);
        }
    }

    private void validateProjectConstraints(
            String defaultProject,
            String defaultTeam,
            String defaultOrg) {

        List<Project> projects = projectService.getProjects(defaultTeam, defaultOrg);

        if (projects.size() > 1) {
            throw new VaradhiException("Lean deployment can not be enabled as there are more than one projects.");
        }
        if (projects.size() == 1 && !defaultProject.equals(projects.get(0).getName())) {
            throw new VaradhiException("Lean deployment can not be enabled as project with different name is present. " +
                    "Please delete the project or change the default project.");
        }
        if (projects.isEmpty()) {
            log.debug("Creating default project as no project is present.");

            projectService.createProject(new Project(defaultProject, 0, "", defaultTeam, defaultOrg));

            log.debug("Created default project as no project is present.");
        }
    }

    private boolean isDefaultTeamPresent(String defaultTeam, String defaultOrg) {
        List<Team> teams = teamService.getTeams(defaultOrg);

        if (teams.size() > 1) {
            throw new VaradhiException("Lean deployment can not be enabled as there are more than one teams.");
        }
        if (teams.size() == 1 && !defaultTeam.equals(teams.get(0).getName())) {
            throw new VaradhiException("Lean deployment can not be enabled as team with different name is present. " +
                    "Please delete the team or change the default team.");
        }
        if (teams.isEmpty()) {
            log.debug("Creating default team and project as no team is present.");

            teamService.createTeam(new Team(defaultTeam, 0, defaultOrg));
            projectService.createProject(new Project(defaultTeam, 0, "", defaultTeam, defaultOrg));

            log.debug("Created default team and project as no team is present.");
            return false;
        }

        return true;
    }

}

