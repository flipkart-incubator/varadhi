package com.flipkart.varadhi.auth;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
public class DefaultAuthorizationConfiguration {
    private List<Role> roles;
    private List<RoleBinding> roleBindings;
}
