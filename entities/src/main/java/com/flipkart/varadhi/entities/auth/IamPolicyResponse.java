package com.flipkart.varadhi.entities.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.VersionedEntity;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Map;
import java.util.Set;

@Getter
@EqualsAndHashCode (callSuper = true)
public class IamPolicyResponse extends VersionedEntity {
    private final Map<String, Set<String>> roleBindings;

    @JsonCreator
    public IamPolicyResponse(@JsonProperty ("name")
    String name, @JsonProperty ("roleBindings")
    Map<String, Set<String>> roleBindings, @JsonProperty ("version")
    int version) {
        super(name, version);
        this.roleBindings = roleBindings;
    }
}
