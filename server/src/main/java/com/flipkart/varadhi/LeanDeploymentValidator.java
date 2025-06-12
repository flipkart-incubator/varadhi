package com.flipkart.varadhi;

import com.flipkart.varadhi.core.config.FeatureFlags;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.common.exceptions.InvalidConfigException;
import com.flipkart.varadhi.core.OrgService;
import com.flipkart.varadhi.core.ProjectService;
import com.flipkart.varadhi.core.TeamService;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class LeanDeploymentValidator {
    private final OrgService orgService;
    private final TeamService teamService;
    private final ProjectService projectService;

    public LeanDeploymentValidator(OrgService orgService, TeamService teamService, ProjectService projectService) {
        this.orgService = orgService;
        this.teamService = teamService;
        this.projectService = projectService;
    }

    public void validate(FeatureFlags options) {
        ensureLeanDeploymentConstraints(options);
    }

    private void ensureLeanDeploymentConstraints(FeatureFlags options) {
        String defaultOrg = options.getDefaultOrg();
        String defaultTeam = options.getDefaultTeam();
        String defaultProject = options.getDefaultProject();

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
            throw new InvalidConfigException(
                String.format(
                    "Lean deployment can not be enabled as org with %s name is present.",
                    orgs.get(0).getName()
                )
            );
        }

        if (orgs.isEmpty()) {
            log.debug("Creating default org no org is present.");
            orgService.createOrg(Org.of(defaultOrg));
            log.debug("Created default org, team and project as no org is present.");
        }
    }

    private void ensureTeamConstraints(String defaultOrg, String defaultTeam) {
        List<Team> teams = teamService.getTeams(defaultOrg);

        if (teams.size() > 1) {
            throw new InvalidConfigException("Lean deployment can not be enabled as there are more than one teams.");
        }
        if (teams.size() == 1 && !defaultTeam.equals(teams.get(0).getName())) {
            throw new InvalidConfigException(
                String.format(
                    "Lean deployment can not be enabled as team with %s name is present.",
                    teams.get(0).getName()
                )
            );
        }
        if (teams.isEmpty()) {
            teamService.createTeam(Team.of(defaultTeam, defaultOrg));
            log.info("Created default team: {}/{} as no team is present.", defaultOrg, defaultTeam);
        }
    }

    private void ensureProjectConstraints(String defaultOrg, String defaultTeam, String defaultProject) {

        List<Project> projects = teamService.getProjects(defaultTeam, defaultOrg);

        if (projects.size() > 1) {
            throw new InvalidConfigException("Lean deployment can not be enabled as there are more than one projects.");
        }
        if (projects.size() == 1 && !defaultProject.equals(projects.get(0).getName())) {
            throw new InvalidConfigException(
                String.format(
                    "Lean deployment can not be enabled as project with %s name is present.",
                    projects.get(0).getName()
                )
            );
        }
        if (projects.isEmpty()) {
            log.debug("Creating default project as no team is present.");
            projectService.createProject(Project.of(defaultProject, "", defaultTeam, defaultOrg));
            log.debug("Created default project as no team is present.");
        }
    }
}
