package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.Project;

import java.util.List;

public interface ProjectMetaStore {
    void createProject(Project project);

    Project getProject(String projectName);

    List<Project> getProjects(String teamName, String orgName);

    boolean checkProjectExists(String projectName);

    void updateProject(Project project);

    void deleteProject(String projectName);
}
