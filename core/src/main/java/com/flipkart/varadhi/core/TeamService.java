package com.flipkart.varadhi.core;

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
        boolean orgExists = metaStore.orgs().exists(team.getOrg());
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format("Org(%s) not found.", team.getOrg()));
        }
        metaStore.teams().create(team);
        return team;
    }

    public Team getTeam(String teamName, String orgName) {
        boolean orgExists = metaStore.orgs().exists(orgName);
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format("Org(%s) not found.", orgName));
        }
        return metaStore.teams().get(teamName, orgName);
    }


    public List<Team> getTeams(String orgName) {
        boolean orgExists = metaStore.orgs().exists(orgName);
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format("Org(%s) not found.", orgName));
        }
        return metaStore.teams().getAll(orgName);
    }

    public List<Project> getProjects(String teamName, String orgName) {
        boolean orgExists = metaStore.orgs().exists(orgName);
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format("Org(%s) not found.", orgName));
        }
        boolean teamExists = metaStore.teams().exists(teamName, orgName);
        if (!teamExists) {
            throw new ResourceNotFoundException(
                String.format("Team(%s) does not exists in the Org(%s).", teamName, orgName)
            );
        }
        return metaStore.projects().getAll(teamName, orgName);
    }


    public void deleteTeam(String teamName, String orgName) {
        boolean orgExists = metaStore.orgs().exists(orgName);
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format("Org(%s) not found.", orgName));
        }
        List<Project> projectsInTeam = metaStore.projects().getAll(teamName, orgName);
        if (!projectsInTeam.isEmpty()) {
            throw new InvalidOperationForResourceException(
                String.format("Can not delete Team(%s) as it has associated Project(s).", teamName)
            );
        }
        metaStore.teams().delete(teamName, orgName);
    }
}
