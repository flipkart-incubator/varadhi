package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.Project;

import java.util.List;

public interface ProjectStore {
    void create(Project project);

    Project get(String projectName);

    List<Project> getAll(String teamName, String orgName);

    boolean exists(String projectName);

    void update(Project project);

    void delete(String projectName);
}
