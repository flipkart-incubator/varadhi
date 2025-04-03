package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStore;

import java.util.List;

public class TeamService {
    private final MetaStore metaStore;

    public TeamService(MetaStore metaStore) {
        this.metaStore = metaStore;
    }

    public Team createTeam(Team team) {
        boolean orgExists = metaStore.orgMetaStore().checkOrgExists(team.getOrg());
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format("Org(%s) not found.", team.getOrg()));
        }
        metaStore.teamMetaStore().createTeam(team);
        return team;
    }

    public Team getTeam(String teamName, String orgName) {
        boolean orgExists = metaStore.orgMetaStore().checkOrgExists(orgName);
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format("Org(%s) not found.", orgName));
        }
        return metaStore.teamMetaStore().getTeam(teamName, orgName);
    }


    public List<Team> getTeams(String orgName) {
        boolean orgExists = metaStore.orgMetaStore().checkOrgExists(orgName);
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format("Org(%s) not found.", orgName));
        }
        return metaStore.teamMetaStore().getTeams(orgName);
    }

    public List<Project> getProjects(String teamName, String orgName) {
        boolean orgExists = metaStore.orgMetaStore().checkOrgExists(orgName);
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format("Org(%s) not found.", orgName));
        }
        boolean teamExists = metaStore.teamMetaStore().checkTeamExists(teamName, orgName);
        if (!teamExists) {
            throw new ResourceNotFoundException(
                String.format("Team(%s) does not exists in the Org(%s).", teamName, orgName)
            );
        }
        return metaStore.projectMetaStore().getProjects(teamName, orgName);
    }


    public void deleteTeam(String teamName, String orgName) {
        boolean orgExists = metaStore.orgMetaStore().checkOrgExists(orgName);
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format("Org(%s) not found.", orgName));
        }
        List<Project> projectsInTeam = metaStore.projectMetaStore().getProjects(teamName, orgName);
        if (projectsInTeam.size() > 0) {
            throw new InvalidOperationForResourceException(
                String.format("Can not delete Team(%s) as it has associated Project(s).", teamName)
            );
        }
        metaStore.teamMetaStore().deleteTeam(teamName, orgName);
    }
}
