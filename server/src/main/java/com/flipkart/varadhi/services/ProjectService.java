package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.exceptions.ArgumentException;
import com.flipkart.varadhi.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStore;

import java.util.List;

import static com.flipkart.varadhi.Constants.INITIAL_VERSION;

public class ProjectService {
    private final MetaStore metaStore;

    public ProjectService(MetaStore metaStore) {
        this.metaStore = metaStore;
    }

    public Project createProject(Project project) {
        boolean orgExists = this.metaStore.checkOrgExists(project.getOrgName());
        if (!orgExists) {
            throw new ResourceNotFoundException(String.format(
                    "Org(%s) not found. For Project creation, associated Org and Team should exist.",
                    project.getOrgName()
            ));
        }
        boolean teamExists = this.metaStore.checkTeamExists(project.getTeamName(), project.getOrgName());
        if (!teamExists) {
            throw new ResourceNotFoundException(String.format(
                    "Team(%s) not found. For Project creation, associated Org and Team should exist.",
                    project.getTeamName()
            ));
        }
        boolean found = this.metaStore.checkProjectExists(project.getName());
        if (found) {
            throw new DuplicateResourceException(
                    String.format("Project(%s) already exists.  Projects are globally unique.", project.getName()));
        }
        project.setVersion(INITIAL_VERSION);
        this.metaStore.createProject(project);
        return project;
    }

    public Project getProject(String projectName) {
        return this.metaStore.getProject(projectName);
    }

    public Project updateProject(Project project) {
        Project existingProject = metaStore.getProject(project.getName());
        if (!project.getOrgName().equals(existingProject.getOrgName())) {
            throw new ArgumentException(
                    String.format("Project(%s) can not be moved across organisation.", project.getName()));
        }
        if (project.getTeamName().equals(existingProject.getTeamName()) &&
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
        int updatedVersion = this.metaStore.updateProject(project);
        project.setVersion(updatedVersion);
        return project;
    }

    public void deleteProject(String projectName) {
        //TODO:: check no subscriptions/queues for this project.
        List<String> varadhiTopicNames = this.metaStore.getVaradhiTopicNames(projectName);
        if (varadhiTopicNames.size() > 0) {
            throw new InvalidOperationForResourceException("Can not delete Project, it has associated entities.");
        }
        this.metaStore.deleteProject(projectName);
    }
}
