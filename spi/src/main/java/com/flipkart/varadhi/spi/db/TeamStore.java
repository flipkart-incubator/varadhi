package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.Team;

import java.util.List;

public interface TeamStore {
    void create(Team team);

    Team get(String teamName, String orgName);

    List<Team> getAll(String orgName);

    List<String> getAllNames(String orgName);

    boolean exists(String teamName, String orgName);

    void delete(String teamName, String orgName);
}
