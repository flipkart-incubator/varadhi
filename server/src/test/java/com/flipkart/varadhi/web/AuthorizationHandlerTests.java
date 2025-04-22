package com.flipkart.varadhi.web;

import com.flipkart.varadhi.server.spi.authz.AuthorizationOptions;
import com.flipkart.varadhi.spi.ConfigFileResolver;

import com.flipkart.varadhi.server.spi.authz.AuthorizationProvider;
import com.flipkart.varadhi.entities.Hierarchies;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.auth.ResourceAction;
import com.flipkart.varadhi.entities.auth.UserContext;
import io.vertx.core.Future;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Objects;

import static com.flipkart.varadhi.entities.Hierarchies.*;
import static com.flipkart.varadhi.entities.TestUser.testUser;

@ExtendWith (VertxExtension.class)
public class AuthorizationHandlerTests {

    private final AuthorizationHandlerBuilder authzHandlerBuilder = new AuthorizationHandlerBuilder(
        new TestAuthorizationProvider()
    );

    @Test
    public void testAuthorizationHandler(VertxTestContext testCtx) {
        Checkpoint checks = testCtx.checkpoint(5);
        Project prj = Project.of("p1", "", "team1", "org1");

        authzHandlerBuilder.build(ResourceAction.TOPIC_CREATE)
                           .authorize(testUser("a", false), new ProjectHierarchy(prj))
                           .onComplete(testCtx.succeeding(v ->
                                   checks.flag()));

        authzHandlerBuilder.build(ResourceAction.SUBSCRIPTION_DELETE)
                           .authorize(testUser("a", true), new SubscriptionHierarchy(prj, "s1"))
                           .onComplete(testCtx.failing(t -> {
                               Assertions.assertEquals(401, ((HttpException)t).getStatusCode());
                               checks.flag();
                           }));

        authzHandlerBuilder.build(ResourceAction.TOPIC_DELETE)
                           .authorize(testUser("alice", false), new Hierarchies.TopicHierarchy(prj, "t1"))
                           .onComplete(testCtx.failing(t -> {
                               Assertions.assertEquals(403, ((HttpException)t).getStatusCode());
                               checks.flag();
                           }));

        authzHandlerBuilder.build(ResourceAction.TOPIC_GET)
                           .authorize(testUser("intern", false), new Hierarchies.TopicHierarchy(prj, "t1"))
                           .onComplete(testCtx.succeeding(v -> checks.flag()));

        authzHandlerBuilder.build(ResourceAction.TOPIC_GET)
                           .authorize(testUser("doom", false), new Hierarchies.TopicHierarchy(prj, "t1"))
                           .onComplete(testCtx.failing(t -> {
                               Assertions.assertEquals(500, ((HttpException)t).getStatusCode());
                               checks.flag();
                           }));
    }

    static class TestAuthorizationProvider implements AuthorizationProvider {
        @Override
        public Future<Boolean> init(ConfigFileResolver resolver, AuthorizationOptions authorizationOptions) {
            return Future.succeededFuture();
        }

        @Override
        public Future<Boolean> isAuthorized(UserContext userContext, ResourceAction action, String resource) {
            if (List.of("a", "superman", "manager", "architect").contains(userContext.getSubject())) {
                return Future.succeededFuture(true);
            } else if (List.of("alice", "bob", "intern").contains(userContext.getSubject()) && action.toString()
                                                                                                     .endsWith("get")) {
                return Future.succeededFuture(true);
            } else if (Objects.equals("doom", userContext.getSubject())) {
                return Future.failedFuture(new RuntimeException("it was destined to be doomed"));
            } else {
                return Future.succeededFuture(false);
            }
        }
    }
}
