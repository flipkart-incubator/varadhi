package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Abstract class representing an entity with a lifecycle status.
 * Extends the MetaStoreEntity class.
 */
@Getter
@Setter
@EqualsAndHashCode (callSuper = true)
public abstract class LifecycleEntity extends MetaStoreEntity {
    protected LifecycleStatus status;

    /**
     * Constructor to initialize the LifecycleEntity with a name and version.
     *
     * @param name    the name of the entity
     * @param version the version of the entity
     */
    protected LifecycleEntity(String name, int version) {
        super(name, version);
    }

    /**
     * Marks the entity as created.
     */
    public void markCreated() {
        this.status.update(LifecycleStatus.State.CREATED);
    }

    /**
     * Marks the entity as failed to create with a message.
     *
     * @param message the failure message
     */
    public void markCreateFailed(String message) {
        this.status.update(LifecycleStatus.State.CREATE_FAILED, message);
    }

    /**
     * Marks the entity as deleting with an actor code and message.
     *
     * @param actorCode the actor code indicating the reason for the state
     * @param message   the message associated with the state
     */
    public void markDeleting(LifecycleStatus.ActorCode actorCode, String message) {
        this.status.update(LifecycleStatus.State.DELETING, message, actorCode);
    }

    /**
     * Marks the entity as failed to delete with a message.
     *
     * @param message the failure message
     */
    public void markDeleteFailed(String message) {
        this.status.update(LifecycleStatus.State.DELETE_FAILED, message);
    }

    /**
     * Marks the entity as inactive with an actor code and message.
     *
     * @param actorCode the actor code indicating the reason for the state
     * @param message   the message associated with the state
     */
    public void markInactive(LifecycleStatus.ActorCode actorCode, String message) {
        this.status.update(LifecycleStatus.State.INACTIVE, message, actorCode);
    }

    /**
     * Restores the entity to the created state with an actor code and message.
     *
     * @param actorCode the actor code indicating the reason for the state
     * @param message   the message associated with the state
     */
    public void restore(LifecycleStatus.ActorCode actorCode, String message) {
        this.status.update(LifecycleStatus.State.CREATED, message, actorCode);
    }

    /**
     * Checks if the entity is active.
     *
     * @return true if the entity is in the created state, false otherwise
     */
    @JsonIgnore
    public boolean isActive() {
        return this.status.getState() == LifecycleStatus.State.CREATED;
    }

    /**
     * Checks if the entity is inactive.
     *
     * @return true if the entity is in the inactive state, false otherwise
     */
    @JsonIgnore
    public boolean isInactive() {
        return this.status.getState() == LifecycleStatus.State.INACTIVE;
    }

    /**
     * Checks if the entity's state is retriable.
     *
     * @return true if the entity's state is retriable, false otherwise
     */
    @JsonIgnore
    public boolean isRetriable() {
        return this.status.getState().isRetriable();
    }
}
