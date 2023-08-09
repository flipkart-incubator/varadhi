package com.flipkart.varadhi.services;

import com.flipkart.varadhi.db.MetaStore;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;

import java.util.List;

import static com.flipkart.varadhi.Constants.INITIAL_VERSION;

public class TeamService {
    private final MetaStore metaStore;

    public TeamService(MetaStore metaStore) {
        this.metaStore = metaStore;
    }

    public Team createTeam(Team team) {
        boolean orgExists = this.metaStore.checkOrgExists(team.getOrgName());
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format("Org(%s) not found.", team.getOrgName()));
        }
        boolean teamExists = this.metaStore.checkTeamExists(team.getName(), team.getOrgName());
        if (teamExists) {
            throw new DuplicateResourceException(
                    String.format("Team(%s) already exists. Team is unique with in Org.", team.getName()));
        }
        team.setVersion(INITIAL_VERSION);
        this.metaStore.createTeam(team);
        return team;
    }

    public Team getTeam(String teamName, String orgName) {
        boolean orgExists = this.metaStore.checkOrgExists(orgName);
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format("Org(%s) not found.", orgName));
        }
        return this.metaStore.getTeam(teamName, orgName);
    }


    public List<Team> getTeams(String orgName) {
        boolean orgExists = this.metaStore.checkOrgExists(orgName);
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format("Org(%s) not found.", orgName));
        }
        return this.metaStore.getTeams(orgName);
    }

    public List<Project> getProjects(String teamName, String orgName) {
        boolean orgExists = this.metaStore.checkOrgExists(orgName);
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format("Org(%s) not found.", orgName));
        }
        boolean teamExists = this.metaStore.checkTeamExists(teamName, orgName);
        if (!teamExists) {
            throw new ResourceNotFoundException(
                    String.format("Team(%s) does not exists in the Org(%s).", teamName, orgName));
        }
        return this.metaStore.getProjects(teamName, orgName);
    }


    public void deleteTeam(String teamName, String orgName) {
        boolean orgExists = this.metaStore.checkOrgExists(orgName);
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format("Org(%s) not found.", orgName));
        }
        List<Project> projectsInTeam = this.metaStore.getProjects(teamName, orgName);
        if (projectsInTeam.size() > 0) {
            throw new InvalidOperationForResourceException(
                    String.format("Can not delete Team(%s) as it has associated Project(s).", teamName));
        }
        this.metaStore.deleteTeam(teamName, orgName);
    }
}
