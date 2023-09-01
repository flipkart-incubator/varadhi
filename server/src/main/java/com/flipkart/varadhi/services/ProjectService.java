package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.exceptions.ArgumentException;
import com.flipkart.varadhi.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStore;

import java.util.List;

public class ProjectService {
    private final MetaStore metaStore;

    public ProjectService(MetaStore metaStore) {
        this.metaStore = metaStore;
    }

    public Project createProject(Project project) {
        boolean orgExists = metaStore.checkOrgExists(project.getOrg());
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format(
                    "Org(%s) not found. For Project creation, associated Org and Team should exist.",
                    project.getOrg()
            ));
        }
        boolean teamExists = metaStore.checkTeamExists(project.getTeam(), project.getOrg());
        if (!teamExists) {
            throw new ResourceNotFoundException(String.format(
                    "Team(%s) not found. For Project creation, associated Org and Team should exist.",
                    project.getTeam()
            ));
        }
        boolean found = metaStore.checkProjectExists(project.getName());
        if (found) {
            throw new DuplicateResourceException(
                    String.format("Project(%s) already exists.  Projects are globally unique.", project.getName()));
        }
        metaStore.createProject(project);
        return project;
    }

    public Project getProject(String projectName) {
        return metaStore.getProject(projectName);
    }

    public Project updateProject(Project project) {
        Project existingProject = metaStore.getProject(project.getName());
        if (!project.getOrg().equals(existingProject.getOrg())) {
            throw new ArgumentException(
                    String.format("Project(%s) can not be moved across organisation.", project.getName()));
        }
        if (project.getTeam().equals(existingProject.getTeam()) &&
                project.getDescription().equals(existingProject.getDescription())) {
            throw new ArgumentException(
                    String.format(
                            "Project(%s) has same team name and description. Nothing to update.",
                            project.getName()
                    ));
        }
        if (project.getVersion() != existingProject.getVersion()) {
            throw new InvalidOperationForResourceException(String.format(
                    "Conflicting update, Project(%s) has been modified. Fetch latest and try again.", project.getName()
            ));
        }
        int updatedVersion = metaStore.updateProject(project);
        project.setVersion(updatedVersion);
        return project;
    }

    public void deleteProject(String projectName) {
        //TODO:: check no subscriptions/queues for this project.
        List<String> varadhiTopicNames = metaStore.getVaradhiTopicNames(projectName);
        if (varadhiTopicNames.size() > 0) {
            throw new InvalidOperationForResourceException(
                    String.format("Can not delete Project(%s), it has associated entities.", projectName));
        }
        metaStore.deleteProject(projectName);
    }
}
