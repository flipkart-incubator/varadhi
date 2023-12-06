package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Set;

@Getter
@EqualsAndHashCode
public class IAMPolicyRequest {
    String subject;
    Set<String> roles;

    public IAMPolicyRequest(String subject, Set<String> roles) {
        this.subject = subject;
        this.roles = roles;
    }
}
