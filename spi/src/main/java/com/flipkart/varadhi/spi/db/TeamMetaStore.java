package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.Team;

import java.util.List;

public interface TeamMetaStore {
    void createTeam(Team team);

    Team getTeam(String teamName, String orgName);

    List<Team> getTeams(String orgName);

    List<String> getTeamNames(String orgName);

    boolean checkTeamExists(String teamName, String orgName);

    void deleteTeam(String teamName, String orgName);
}
