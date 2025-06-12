package com.flipkart.varadhi.web.authz;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.auth.IamPolicyRecord;
import com.flipkart.varadhi.entities.auth.ResourceAction;
import com.flipkart.varadhi.services.IamPolicyService;
import com.flipkart.varadhi.spi.ConfigFileResolver;
import com.flipkart.varadhi.web.spi.authz.AuthorizationOptions;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.stubbing.answers.ThrowsException;

import static com.flipkart.varadhi.entities.TestUser.testUser;
import static com.flipkart.varadhi.utils.IamPolicyHelper.getAuthResourceFQN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith (VertxExtension.class)
class DefaultAuthorizationProviderTest {

    private static final ConfigFileResolver ID = p -> p;

    @TempDir
    private Path tempDir;

    private AuthorizationOptions authorizationOptions;

    private DefaultAuthorizationProvider provider;

    private IamPolicyService iamPolicyService;

    private MeterRegistry meterRegistry;

    @BeforeEach
    public void setUp() throws IOException {
        authorizationOptions = new AuthorizationOptions();
        authorizationOptions.setConfigFile("testAuthorizationConfig.yml");

        provider = spy(new DefaultAuthorizationProvider());
        meterRegistry = new SimpleMeterRegistry();
        iamPolicyService = mock(
            IamPolicyService.class,
            new ThrowsException(new ResourceNotFoundException("resource not found"))
        );
    }

    @Test
    void testInit(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);
        provider.init(ID, authorizationOptions, meterRegistry).onComplete(testContext.succeeding(t -> {
            Assertions.assertTrue(t);
            checkpoint.flag();
        }));
    }

    @Test
    void testInitNotImplIamPoliciesShouldThrow() throws IOException {
        Path configFile = tempDir.resolve("config.yaml");
        String yamlContent = """
                    metaStoreOptions:
                      providerClassName: "com.flipkart.varadhi.utils.InvalidMetaStoreProvider"
                      configFile: ""
                    roleDefinitions:
                      org.admin:
                        roleId: org.admin
                        permissions:
                            - ORG_CREATE
            """;
        Files.write(configFile, yamlContent.getBytes());

        AuthorizationOptions opts = new AuthorizationOptions();
        opts.setConfigFile(configFile.toString());
        Assertions.assertThrows(IllegalStateException.class, () -> provider.init(ID, opts, meterRegistry));
    }

    @Test
    void testNotInit() {
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> provider.isAuthorized(TestUser.testUser("abc", false), ResourceAction.ORG_UPDATE, "flipkart")
        );
    }

    @Test
    void testIsAuthorizedForAuthorizedUserOnOrgReturnsTrue(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        String resourceId = "flipkart";
        var resourceTypeCaptor = ArgumentCaptor.forClass(ResourceType.class);
        var resourceIdCaptor = ArgumentCaptor.forClass(String.class);

        doReturn(
            new IamPolicyRecord(
                getAuthResourceFQN(ResourceType.ORG, resourceId),
                1,
                Map.of(userName, Set.of("org.admin"))
            )
        ).when(iamPolicyService).getIamPolicy(resourceTypeCaptor.capture(), resourceIdCaptor.capture());
        doReturn(iamPolicyService).when(provider).getAuthZService();
        provider.init(ID, authorizationOptions, meterRegistry).onComplete(testContext.succeeding(t -> {
            Assertions.assertTrue(t);
            checkpoint.flag();
        }))
                .compose(
                    t -> provider.isAuthorized(
                        TestUser.testUser(userName, false),
                        ResourceAction.ORG_UPDATE,
                        resourceId
                    )
                )
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    assertEquals(ResourceType.ORG, resourceTypeCaptor.getValue());
                    assertEquals(resourceId, resourceIdCaptor.getValue());
                    checkpoint.flag();
                }));
    }

    @Test
    void testIsAuthorizedForEmptyPathReturnFalse(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        doReturn(iamPolicyService).when(provider).getAuthZService();
        provider.init(ID, authorizationOptions, meterRegistry).onComplete(testContext.succeeding(t -> {
            Assertions.assertTrue(t);
            checkpoint.flag();
        }))
                .compose(t -> provider.isAuthorized(TestUser.testUser(userName, false), ResourceAction.ORG_UPDATE, ""))
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    verifyNoInteractions(iamPolicyService);
                    checkpoint.flag();
                }));
    }

    @Test
    void testIsAuthorizedForNullPathReturnFalse(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        doReturn(iamPolicyService).when(provider).getAuthZService();
        provider.init(ID, authorizationOptions, meterRegistry).onComplete(testContext.succeeding(t -> {
            Assertions.assertTrue(t);
            checkpoint.flag();
        }))
                .compose(
                    t -> provider.isAuthorized(TestUser.testUser(userName, false), ResourceAction.ORG_UPDATE, null)
                )
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    verifyNoInteractions(iamPolicyService);
                    checkpoint.flag();
                }));
    }

    @Test
    void testIsAuthorizedForAuthorizedUserOnTeamReturnsTrue(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        String resourceId = "flipkart:team_a";
        String resourcePath = "flipkart/team_a";
        var resourceIdCaptor = ArgumentCaptor.forClass(String.class);

        doReturn(
            new IamPolicyRecord(
                getAuthResourceFQN(ResourceType.TEAM, resourceId),
                1,
                Map.of(userName, Set.of("team.admin"))
            )
        ).when(iamPolicyService).getIamPolicy(eq(ResourceType.TEAM), resourceIdCaptor.capture());

        doReturn(iamPolicyService).when(provider).getAuthZService();
        provider.init(ID, authorizationOptions, meterRegistry).onComplete(testContext.succeeding(t -> {
            Assertions.assertTrue(t);
            checkpoint.flag();
        }))
                .compose(
                    t -> provider.isAuthorized(
                        TestUser.testUser(userName, false),
                        ResourceAction.TEAM_UPDATE,
                        resourcePath
                    )
                )
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    assertEquals(resourceId, resourceIdCaptor.getValue());
                    // since is auth was true on team, we should not check for org
                    verify(iamPolicyService, times(1)).getIamPolicy(eq(ResourceType.TEAM), eq(resourceId));
                    verify(iamPolicyService, times(0)).getIamPolicy(eq(ResourceType.ORG), eq("flipkart"));
                    checkpoint.flag();
                }));
    }

    @Test
    void testIsAuthorizedForAuthorizedUserOnOrgForTeamReturnsTrue(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        String resourceId = "flipkart:team_a";
        String resourcePath = "flipkart/team_a";

        doReturn(
            new IamPolicyRecord(
                getAuthResourceFQN(ResourceType.TEAM, resourceId),
                1,
                Map.of(userName, Set.of("team.reader"))
            )
        ).when(iamPolicyService).getIamPolicy(eq(ResourceType.TEAM), anyString());

        doReturn(
            new IamPolicyRecord(
                getAuthResourceFQN(ResourceType.ORG, "flipkart"),
                1,
                Map.of(userName, Set.of("org.admin"))
            )
        ).when(iamPolicyService).getIamPolicy(eq(ResourceType.ORG), anyString());

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(ID, authorizationOptions, meterRegistry).onComplete(testContext.succeeding(t -> {
            Assertions.assertTrue(t);
            checkpoint.flag();
        }))
                .compose(
                    t -> provider.isAuthorized(
                        TestUser.testUser(userName, false),
                        ResourceAction.TEAM_UPDATE,
                        resourcePath
                    )
                )
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    verify(iamPolicyService, times(1)).getIamPolicy(eq(ResourceType.TEAM), eq(resourceId));
                    verify(iamPolicyService, times(1)).getIamPolicy(eq(ResourceType.ORG), eq("flipkart"));
                    checkpoint.flag();
                }));
    }

    @Test
    void testIsAuthorizedForNotAuthCaseOnProjectReturnsFalse(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        String resourcePath = "flipkart/team_a/proj_a";
        doReturn(
            new IamPolicyRecord(
                getAuthResourceFQN(ResourceType.TEAM, "flipkart:team_a"),
                1,
                Map.of(userName, Set.of("team.reader"))
            )
        ).when(iamPolicyService).getIamPolicy(eq(ResourceType.TEAM), anyString());

        doReturn(new IamPolicyRecord(getAuthResourceFQN(ResourceType.PROJECT, "proj_a"), 1, Map.of())).when(
            iamPolicyService
        ).getIamPolicy(eq(ResourceType.PROJECT), anyString());

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(ID, authorizationOptions, meterRegistry).onComplete(testContext.succeeding(t -> {
            Assertions.assertTrue(t);
            checkpoint.flag();
        }))
                .compose(
                    t -> provider.isAuthorized(
                        TestUser.testUser(userName, false),
                        ResourceAction.PROJECT_UPDATE,
                        resourcePath
                    )
                )
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    checkpoint.flag();
                }));
    }

    @Test
    void testIsAuthorizedForAuthCaseOnProjectReturnsTrue(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        String resourcePath = "flipkart/team_a/proj_a";

        doReturn(
            new IamPolicyRecord(
                getAuthResourceFQN(ResourceType.TEAM, "flipkart:team_a"),
                1,
                Map.of(userName, Set.of("team.reader", "project.writer"))
            )
        ).when(iamPolicyService).getIamPolicy(eq(ResourceType.TEAM), anyString());

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(ID, authorizationOptions, meterRegistry).onComplete(testContext.succeeding(t -> {
            Assertions.assertTrue(t);
            checkpoint.flag();
        }))
                .compose(
                    t -> provider.isAuthorized(
                        TestUser.testUser(userName, false),
                        ResourceAction.PROJECT_UPDATE,
                        resourcePath
                    )
                )
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }

    @Test
    void testIsAuthorizedForAuthCaseOnTopicReturnsTrue(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        String resourcePath = "flipkart/team_a/proj_a/topic_a";

        doReturn(
            new IamPolicyRecord(
                getAuthResourceFQN(ResourceType.PROJECT, "proj_a"),
                1,
                Map.of(userName, Set.of("project.writer", "topic.admin"))
            )
        ).when(iamPolicyService).getIamPolicy(eq(ResourceType.PROJECT), anyString());

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(ID, authorizationOptions, meterRegistry).onComplete(testContext.succeeding(t -> {
            Assertions.assertTrue(t);
            checkpoint.flag();
        }))
                .compose(
                    t -> provider.isAuthorized(
                        TestUser.testUser(userName, false),
                        ResourceAction.TOPIC_DELETE,
                        resourcePath
                    )
                )
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }

    @Test
    void testIsAuthorizedForNotAuthCaseOnTopicReturnsFalse(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        String resourcePath = "flipkart/team_a/proj_a/topic_a";

        doReturn(
            new IamPolicyRecord(
                getAuthResourceFQN(ResourceType.PROJECT, "proj_a"),
                1,
                Map.of(userName, Set.of("project.writer", "topic.reader"))
            )
        ).when(iamPolicyService).getIamPolicy(eq(ResourceType.PROJECT), eq("proj_a"));
        doReturn(
            new IamPolicyRecord(
                getAuthResourceFQN(ResourceType.TOPIC, "proj_a:topic_a"),
                1,
                Map.of(userName, Set.of("topic.reader"))
            )
        ).when(iamPolicyService).getIamPolicy(eq(ResourceType.TOPIC), eq("proj_a:topic_a"));
        doReturn(
            new IamPolicyRecord(
                getAuthResourceFQN(ResourceType.TOPIC, "proj_a:topic_b"),
                1,
                Map.of(userName, Set.of("topic.admin"))
            )
        ).when(iamPolicyService).getIamPolicy(eq(ResourceType.TOPIC), eq("proj_a:topic_b"));

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(ID, authorizationOptions, meterRegistry).onComplete(testContext.succeeding(t -> {
            Assertions.assertTrue(t);
            checkpoint.flag();
        }))
                .compose(
                    t -> provider.isAuthorized(
                        TestUser.testUser(userName, false),
                        ResourceAction.TOPIC_UPDATE,
                        resourcePath
                    )
                )
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    checkpoint.flag();
                }));
    }

    @Test
    void testIsAuthorizedForAuthCaseOnSubReturnsTrue(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        String resourcePath = "flipkart/team_a/proj_a/sub_a";

        doReturn(
            new IamPolicyRecord(
                getAuthResourceFQN(ResourceType.SUBSCRIPTION, "sub_a"),
                1,
                Map.of(userName, Set.of("project.writer", "subscription.admin"))
            )
        ).when(iamPolicyService).getIamPolicy(eq(ResourceType.SUBSCRIPTION), anyString());

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(ID, authorizationOptions, meterRegistry).onComplete(testContext.succeeding(t -> {
            Assertions.assertTrue(t);
            checkpoint.flag();
        }))
                .compose(
                    t -> provider.isAuthorized(
                        TestUser.testUser(userName, false),
                        ResourceAction.SUBSCRIPTION_DELETE,
                        resourcePath
                    )
                )
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }

    @Test
    void testIsAuthorizedForNotAuthCaseOnSubReturnsFalse(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        String resourcePath = "flipkart/team_a/proj_a/sub_a";

        doReturn(
            new IamPolicyRecord(
                getAuthResourceFQN(ResourceType.PROJECT, "proj_a"),
                1,
                Map.of(userName, Set.of("project.writer", "subscription.reader"))
            )
        ).when(iamPolicyService).getIamPolicy(eq(ResourceType.PROJECT), anyString());
        doReturn(
            new IamPolicyRecord(
                getAuthResourceFQN(ResourceType.SUBSCRIPTION, "proj_a:sub_a"),
                1,
                Map.of(userName, Set.of("subscription.reader"))
            )
        ).when(iamPolicyService).getIamPolicy(eq(ResourceType.SUBSCRIPTION), eq("proj_a:sub_a"));
        doReturn(
            new IamPolicyRecord(
                getAuthResourceFQN(ResourceType.SUBSCRIPTION, "proj_a:sub_a"),
                1,
                Map.of(userName, Set.of("subscription.admin"))
            )
        ).when(iamPolicyService).getIamPolicy(eq(ResourceType.SUBSCRIPTION), eq("proj_a:sub_b"));

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(ID, authorizationOptions, meterRegistry).onComplete(testContext.succeeding(t -> {
            Assertions.assertTrue(t);
            checkpoint.flag();
        }))
                .compose(
                    t -> provider.isAuthorized(
                        TestUser.testUser(userName, false),
                        ResourceAction.SUBSCRIPTION_UPDATE,
                        resourcePath
                    )
                )
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    checkpoint.flag();
                }));
    }

    @Test
    void testIsAuthorizedForExtraPathSegmentIsIgnored(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        String resourcePath = "flipkart/team_a/proj_a/topic_a/wut?";

        doReturn(
            new IamPolicyRecord(
                getAuthResourceFQN(ResourceType.TOPIC, "proj_a:topic_a"),
                1,
                Map.of(userName, Set.of("topic.admin"))
            )
        ).when(iamPolicyService).getIamPolicy(eq(ResourceType.TOPIC), eq("proj_a:topic_a"));

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(ID, authorizationOptions, meterRegistry).onComplete(testContext.succeeding(t -> {
            Assertions.assertTrue(t);
            checkpoint.flag();
        }))
                .compose(
                    t -> provider.isAuthorized(
                        TestUser.testUser(userName, false),
                        ResourceAction.TOPIC_UPDATE,
                        resourcePath
                    )
                )
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }

    @Test
    void testIsAuthorizedForInvalidLeaf(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        String resourcePath = "flipkart/team_a/proj_a/topic_a";

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(ID, authorizationOptions, meterRegistry).onComplete(testContext.succeeding(t -> {
            Assertions.assertTrue(t);
            checkpoint.flag();
        }))
                .compose(
                    t -> provider.isAuthorized(
                        TestUser.testUser(userName, false),
                        ResourceAction.TEAM_CREATE,
                        resourcePath
                    )
                )
                .onComplete(testContext.failing(t -> {
                    Assertions.assertInstanceOf(IllegalArgumentException.class, t);
                    checkpoint.flag();
                }));
    }
}
