package com.flipkart.varadhi.deployment;


import com.flipkart.varadhi.VerticleDeployer;
import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.core.cluster.MessageChannel;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.services.MessagingStackProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class LeanDeploymentVerticleDeployer extends VerticleDeployer {
    public LeanDeploymentVerticleDeployer(
            Vertx vertx, AppConfiguration configuration,
            MessagingStackProvider messagingStackProvider, MetaStoreProvider metaStoreProvider,
            MessageChannel messageChannel, MeterRegistry meterRegistry, Tracer tracer
    ) {
        super(vertx, configuration, messagingStackProvider, metaStoreProvider, messageChannel, meterRegistry, tracer);
    }

    @Override
    public Future<String> deployVerticle(
            Vertx vertx,
            AppConfiguration configuration
    ) {
        Promise<String> promise = Promise.promise();
        vertx.executeBlocking(future -> {
            ensureLeanDeploymentConstraints(configuration.getRestOptions());
            future.complete();
        }, promise);
        return promise.future().compose(v -> super.deployVerticle(vertx, configuration));
    }

    private void ensureLeanDeploymentConstraints(RestOptions restOptions) {
        String defaultOrg = restOptions.getDefaultOrg();
        String defaultTeam = restOptions.getDefaultTeam();
        String defaultProject = restOptions.getDefaultProject();

        ensureOrgConstraints(defaultOrg);
        ensureTeamConstraints(defaultOrg, defaultTeam);
        ensureProjectConstraints(defaultOrg, defaultTeam, defaultProject);
    }

    private void ensureOrgConstraints(String defaultOrg) {

        List<Org> orgs = orgService.getOrgs();

        if (orgs.size() > 1) {
            throw new InvalidConfigException("Lean deployment can not be enabled as there are more than one orgs.");
        }

        if (orgs.size() == 1 && !defaultOrg.equals(orgs.get(0).getName())) {
            throw new InvalidConfigException(String.format(
                    "Lean deployment can not be enabled as org with %s name is present.",
                    orgs.get(0).getName()
            ));
        }

        if (orgs.isEmpty()) {
            log.debug("Creating default org no org is present.");
            orgService.createOrg(new Org(defaultOrg, 0));
            log.debug("Created default org, team and project as no org is present.");
        }
    }

    private void ensureTeamConstraints(String defaultOrg, String defaultTeam) {
        List<Team> teams = teamService.getTeams(defaultOrg);

        if (teams.size() > 1) {
            throw new InvalidConfigException("Lean deployment can not be enabled as there are more than one teams.");
        }
        if (teams.size() == 1 && !defaultTeam.equals(teams.get(0).getName())) {
            throw new InvalidConfigException(String.format(
                    "Lean deployment can not be enabled as team with %s name is present.",
                    teams.get(0).getName()
            ));
        }
        if (teams.isEmpty()) {
            log.debug("Creating default team no team is present.");

            teamService.createTeam(new Team(defaultTeam, 0, defaultOrg));

            log.debug("Created default team as no team is present.");
        }

    }

    private void ensureProjectConstraints(
            String defaultOrg,
            String defaultTeam,
            String defaultProject
    ) {

        List<Project> projects = teamService.getProjects(defaultTeam, defaultOrg);

        if (projects.size() > 1) {
            throw new InvalidConfigException("Lean deployment can not be enabled as there are more than one projects.");
        }
        if (projects.size() == 1 && !defaultProject.equals(projects.get(0).getName())) {
            throw new InvalidConfigException(String.format(
                    "Lean deployment can not be enabled as project with %s name is present.",
                    projects.get(0).getName()
            ));
        }
        if (projects.isEmpty()) {
            log.debug("Creating default project as no team is present.");
            projectService.createProject(new Project(defaultProject, 0, "", defaultTeam, defaultOrg));
            log.debug("Created default project as no team is present.");
        }
    }

}

