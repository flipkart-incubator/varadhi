package com.flipkart.varadhi.entities.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.Validatable;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Set;

@Getter
@EqualsAndHashCode
public class IamPolicyRequest implements Validatable {
    @NotBlank String subject;

    @NotNull Set<String> roles;

    @JsonCreator
    public IamPolicyRequest(@JsonProperty ("subject") String subject, @JsonProperty ("roles") Set<String> roles) {
        this.subject = subject;
        this.roles = roles;
    }
}
