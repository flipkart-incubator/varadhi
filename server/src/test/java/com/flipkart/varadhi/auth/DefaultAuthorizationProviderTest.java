package com.flipkart.varadhi.auth;

import com.flipkart.varadhi.config.AuthorizationOptions;
import com.flipkart.varadhi.entities.ResourceAction;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.exceptions.NotInitializedException;
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

import static com.flipkart.varadhi.entities.TestUser.testUser;

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
                        roleDefinitions:
                          org.admin:
                            roleId: org.admin
                            permissions:
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
                            roleId: team.admin
                            permissions:
                                - TEAM_CREATE
                                - TEAM_GET
                                - TEAM_UPDATE
                                - PROJECT_GET
                                - TOPIC_GET
                          project.read:
                            roleId: project.read
                            permissions:
                                - PROJECT_GET
                                - TOPIC_GET
                          topic.read:
                            roleId: topic.read
                            permissions:
                                - TOPIC_GET
                        """;
        Path configFile = tempDir.resolve("authorizationConfig.yaml");
        Files.write(configFile, configContent.getBytes());

        authorizationOptions = new AuthorizationOptions();
        authorizationOptions.setConfigFile(configFile.toString());

        defaultAuthorizationProvider = new DefaultAuthorizationProvider();
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
    public void testNotInit() {
        Assertions.assertThrows(NotInitializedException.class, () ->
                defaultAuthorizationProvider.isAuthorized(
                        testUser("abc", false), ResourceAction.ORG_UPDATE, "flipkart"));
    }
}
