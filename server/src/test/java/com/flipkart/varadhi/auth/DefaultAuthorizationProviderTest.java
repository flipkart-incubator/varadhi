package com.flipkart.varadhi.auth;

import com.flipkart.varadhi.util.TestUtil;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.mockito.Mockito.spy;

@ExtendWith(VertxExtension.class)
public class DefaultAuthorizationProviderTest {

    @TempDir
    Path tempDir;

    private AuthorizationOptions authorizationOptions;

    private DefaultAuthorizationProvider defaultAuthorizationProvider;

    @BeforeEach
    public void setUp() throws IOException {
        String configContent =
                """
                        ---
                        roles:
                          org.admin:
                            - ORG_CREATE
                            - ORG_UPDATE
                            - ORG_GET
                            - ORG_DELETE
                            - TEAM_CREATE
                            - TEAM_GET
                            - TEAM_UPDATE
                            - PROJECT_GET
                            - TOPIC_GET
                          team.admin:
                            - TEAM_CREATE
                            - TEAM_GET
                            - TEAM_UPDATE
                            - PROJECT_GET
                            - TOPIC_GET
                          project.read:
                            - PROJECT_GET
                            - TOPIC_GET
                          topic.read:
                            - TOPIC_GET
                        roleBindings:
                          varadhi.root:
                            root_usr:
                              - org.admin
                          flipkart:
                            abc:
                              - team.admin
                            xyz:
                              - org.admin
                          flipkart:team_rocket:
                            team_user1:
                              - team.admin
                          flipkart:team_ash:
                            brock:
                              - team.admin
                          proj001:
                            proj_user1:
                              - project.read
                            proj_user2:
                              - topic.read
                          proj001:topic001:
                            proj_user3:
                              - topic.read
                                
                        """;
        Path configFile = tempDir.resolve("authorizationConfig.yaml");
        Files.write(configFile, configContent.getBytes());

        authorizationOptions = new AuthorizationOptions();
        authorizationOptions.setUseDefaultProvider(true);
        authorizationOptions.setConfigFile(configFile.toString());

        defaultAuthorizationProvider = spy(new DefaultAuthorizationProvider());
    }

    @Test
    public void testInit(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);
        defaultAuthorizationProvider.init(authorizationOptions)
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserNotAuthorisedOnResource(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);

        defaultAuthorizationProvider
                .init(authorizationOptions)
                .compose(t -> defaultAuthorizationProvider.isAuthorized(TestUtil.testUser("abc", false),
                        ResourceAction.ORG_CREATE, "flipkart"
                ))
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserAuthorisedOnResource(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);

        defaultAuthorizationProvider
                .init(authorizationOptions)
                .compose(t -> defaultAuthorizationProvider.isAuthorized(TestUtil.testUser("xyz", false),
                        ResourceAction.ORG_CREATE, "flipkart"
                ))
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserRootNodeRoles(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);

        defaultAuthorizationProvider
                .init(authorizationOptions)
                .compose(t -> defaultAuthorizationProvider.isAuthorized(TestUtil.testUser("root_usr", false),
                        ResourceAction.ORG_CREATE, ""
                ))
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))

                .compose(t -> defaultAuthorizationProvider.isAuthorized(TestUtil.testUser("xyz", false),
                        ResourceAction.ORG_CREATE, ""
                )) // xyz has org.admin but not at root level
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserTopicAccess(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(5);

        defaultAuthorizationProvider
                .init(authorizationOptions)
                .compose(t -> defaultAuthorizationProvider.isAuthorized(TestUtil.testUser("proj_user3", false),
                        ResourceAction.TOPIC_GET, "flipkart/team_rocket/proj001/topic001"
                )) // checking if user role at the leaf node resolves
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))

                .compose(t -> defaultAuthorizationProvider.isAuthorized(TestUtil.testUser("proj_user2", false),
                        ResourceAction.TOPIC_GET, "flipkart/team_rocket/proj001/topic001"
                )) // checking if user role at the parent node resolves
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))

                .compose(t -> defaultAuthorizationProvider.isAuthorized(TestUtil.testUser("abc", false),
                        ResourceAction.TOPIC_GET, "flipkart/team_rocket/proj001/topic001"
                )) // checking since abc is team.admin, they should be able to read the topic
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))

                .compose(t -> defaultAuthorizationProvider.isAuthorized(TestUtil.testUser("team_user1", false),
                        ResourceAction.TOPIC_GET, "flipkart/team_rocket/proj001/topic001"
                )) // checking since team_user1 is team.admin, they should be able to read the topic
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))

                .compose(t -> defaultAuthorizationProvider.isAuthorized(TestUtil.testUser("brock", false),
                        ResourceAction.TOPIC_GET, "flipkart/team_rocket/proj001/topic001"
                )) // brock is team admin for different team, should not be able to access
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserProjectAccess(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(3);
        defaultAuthorizationProvider
                .init(authorizationOptions)
                .compose(t -> defaultAuthorizationProvider.isAuthorized(TestUtil.testUser("proj_user2", false),
                        ResourceAction.PROJECT_GET, "flipkart/team_rocket/proj001"
                )) // proj_user2 only has topic read access, so should fail
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    checkpoint.flag();
                }))

                .compose(t -> defaultAuthorizationProvider.isAuthorized(TestUtil.testUser("proj_user2", false),
                        ResourceAction.TOPIC_GET, "flipkart/team_rocket/proj001"
                )) // proj_user2 only has topic read access, so should fail
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))

                .compose(t -> defaultAuthorizationProvider.isAuthorized(TestUtil.testUser("proj_user1", false),
                        ResourceAction.PROJECT_GET, "flipkart/team_rocket/proj001"
                )) // proj_user1 is project.read so should work
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }
}
