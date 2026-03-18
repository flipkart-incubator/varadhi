package com.flipkart.varadhi.web.routes;

/**
 * Base paths for API route registration. Shared by handlers that mount routes under the same path.
 */
public final class ApiPaths {

    /** Base path for subscription and subscription-action routes (CRUD, restore, start, stop, etc.). */
    public static final String SUBSCRIPTIONS = "/v1/projects/:project/subscriptions";

    private ApiPaths() {
    }
}
