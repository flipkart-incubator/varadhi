package com.flipkart.varadhi.entities.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Set;

@Getter
@EqualsAndHashCode
public class IAMPolicyRequest {
    String subject;
    Set<String> roles;

    @JsonCreator
    public IAMPolicyRequest(@JsonProperty("subject") String subject, @JsonProperty("roles") Set<String> roles) {
        this.subject = subject;
        this.roles = roles;
    }
}
