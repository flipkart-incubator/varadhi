package com.flipkart.varadhi.auth;

import com.flipkart.varadhi.authz.AuthorizationOptions;
import com.flipkart.varadhi.entities.auth.ResourceAction;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.auth.RoleBindingNode;
import com.flipkart.varadhi.services.IamPolicyService;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static com.flipkart.varadhi.entities.TestUser.testUser;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
public class DefaultAuthorizationProviderTest {

    @TempDir
    private Path tempDir;

    private AuthorizationOptions authorizationOptions;

    private DefaultAuthorizationProvider provider;

    private IamPolicyService iamPolicyService;

    @BeforeEach
    public void setUp() throws IOException {
        authorizationOptions = new AuthorizationOptions();
        authorizationOptions.setConfigFile("src/test/resources/testAuthorizationConfig.yml");

        provider = spy(new DefaultAuthorizationProvider());
        iamPolicyService = mock(IamPolicyService.class);
    }

    @Test
    public void testInit(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);
        provider.init(authorizationOptions)
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testInitNotImplRoleBindingMetaStoreShouldThrow() throws IOException {
        Path configFile = tempDir.resolve("config.yaml");
        String yamlContent =
                """
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
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> provider.init(opts)
        );
    }

    @Test
    public void testNotInit() {
        Assertions.assertThrows(IllegalStateException.class, () ->
                provider.isAuthorized(
                        testUser("abc", false), ResourceAction.ORG_UPDATE, "flipkart"));
    }

    @Test
    void testIsAuthorizedForAuthorizedUserOnOrgReturnsTrue(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        String resourceId = "flipkart";
        var resourceTypeCaptor = ArgumentCaptor.forClass(ResourceType.class);
        var resourceIdCaptor = ArgumentCaptor.forClass(String.class);
        when(iamPolicyService.getIAMPolicy(resourceTypeCaptor.capture(), resourceIdCaptor.capture()))
                .thenReturn(
                        new RoleBindingNode(resourceId, ResourceType.ORG, Map.of(userName, Set.of("org.admin")), 1));
        doReturn(iamPolicyService).when(provider).getAuthZService();
        provider.init(authorizationOptions)
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))
                .compose(t -> provider.isAuthorized(
                        testUser(userName, false), ResourceAction.ORG_UPDATE, resourceId)
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
        provider.init(authorizationOptions)
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))
                .compose(t -> provider.isAuthorized(
                        testUser(userName, false), ResourceAction.ORG_UPDATE, "")
                )
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
        provider.init(authorizationOptions)
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))
                .compose(t -> provider.isAuthorized(
                        testUser(userName, false), ResourceAction.ORG_UPDATE, null)
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
        when(iamPolicyService.getIAMPolicy(eq(ResourceType.TEAM), resourceIdCaptor.capture()))
                .thenReturn(
                        new RoleBindingNode(resourceId, ResourceType.TEAM, Map.of(userName, Set.of("team.admin")), 1));
        doReturn(iamPolicyService).when(provider).getAuthZService();
        provider.init(authorizationOptions)
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))
                .compose(t -> provider.isAuthorized(
                        testUser(userName, false), ResourceAction.TEAM_UPDATE, resourcePath)
                )
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    assertEquals(resourceId, resourceIdCaptor.getValue());
                    // since is auth was true on team, we should not check for org
                    verify(iamPolicyService, times(1)).getIAMPolicy(eq(ResourceType.TEAM), eq(resourceId));
                    verify(iamPolicyService, times(0)).getIAMPolicy(eq(ResourceType.ORG), eq("flipkart"));
                    checkpoint.flag();
                }));
    }

    @Test
    void testIsAuthorizedForAuthorizedUserOnOrgForTeamReturnsTrue(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        String resourceId = "flipkart:team_a";
        String resourcePath = "flipkart/team_a";
        when(iamPolicyService.getIAMPolicy(eq(ResourceType.TEAM), anyString()))
                .thenReturn(
                        new RoleBindingNode(resourceId, ResourceType.TEAM, Map.of(userName, Set.of("team.reader")), 1));
        when(iamPolicyService.getIAMPolicy(eq(ResourceType.ORG), anyString()))
                .thenReturn(
                        new RoleBindingNode("flipkart", ResourceType.ORG, Map.of(userName, Set.of("org.admin")), 1));

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(authorizationOptions)
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))
                .compose(t -> provider.isAuthorized(
                        testUser(userName, false), ResourceAction.TEAM_UPDATE, resourcePath)
                )
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    verify(iamPolicyService, times(1)).getIAMPolicy(eq(ResourceType.TEAM), eq(resourceId));
                    verify(iamPolicyService, times(1)).getIAMPolicy(eq(ResourceType.ORG), eq("flipkart"));
                    checkpoint.flag();
                }));
    }

    @Test
    void testIsAuthorizedForNotAuthCaseOnProjectReturnsFalse(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        String userName = "abc";
        String resourcePath = "flipkart/team_a/proj_a";
        when(iamPolicyService.getIAMPolicy(eq(ResourceType.TEAM), anyString()))
                .thenReturn(new RoleBindingNode("flipkart:team_a", ResourceType.TEAM,
                        Map.of(userName, Set.of("team.reader")), 1
                ));
        when(iamPolicyService.getIAMPolicy(eq(ResourceType.PROJECT), anyString()))
                .thenReturn(new RoleBindingNode("proj_a", ResourceType.PROJECT, Map.of(), 1));

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(authorizationOptions)
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))
                .compose(t -> provider.isAuthorized(
                        testUser(userName, false), ResourceAction.PROJECT_UPDATE, resourcePath)
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
        when(iamPolicyService.getIAMPolicy(eq(ResourceType.TEAM), anyString()))
                .thenReturn(new RoleBindingNode("flipkart:team_a", ResourceType.TEAM,
                        Map.of(userName, Set.of("team.reader", "project.writer")), 1
                ));

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(authorizationOptions)
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))
                .compose(t -> provider.isAuthorized(
                        testUser(userName, false), ResourceAction.PROJECT_UPDATE, resourcePath)
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
        when(iamPolicyService.getIAMPolicy(eq(ResourceType.PROJECT), anyString()))
                .thenReturn(new RoleBindingNode("proj_a", ResourceType.PROJECT,
                        Map.of(userName, Set.of("project.writer", "topic.admin")), 1
                ));

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(authorizationOptions)
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))
                .compose(t -> provider.isAuthorized(
                        testUser(userName, false), ResourceAction.TOPIC_DELETE, resourcePath)
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
        when(iamPolicyService.getIAMPolicy(eq(ResourceType.PROJECT), anyString()))
                .thenReturn(new RoleBindingNode("proj_a", ResourceType.PROJECT,
                        Map.of(userName, Set.of("project.writer", "topic.reader")), 1
                ));
        when(iamPolicyService.getIAMPolicy(eq(ResourceType.TOPIC), eq("proj_a:topic_a")))
                .thenReturn(new RoleBindingNode("proj_a:topic_a", ResourceType.TOPIC,
                        Map.of(userName, Set.of("topic.reader")), 1
                ));
        when(iamPolicyService.getIAMPolicy(eq(ResourceType.TOPIC), eq("proj_a:topic_b")))
                .thenReturn(new RoleBindingNode("proj_a:topic_b", ResourceType.TOPIC,
                        Map.of(userName, Set.of("topic.admin")), 1
                ));

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(authorizationOptions)
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))
                .compose(t -> provider.isAuthorized(
                        testUser(userName, false), ResourceAction.TOPIC_UPDATE, resourcePath)
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
        when(iamPolicyService.getIAMPolicy(eq(ResourceType.SUBSCRIPTION), anyString()))
                .thenReturn(new RoleBindingNode("sub_a", ResourceType.SUBSCRIPTION,
                        Map.of(userName, Set.of("project.writer", "subscription.admin")), 1
                ));

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(authorizationOptions)
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))
                .compose(t -> provider.isAuthorized(
                        testUser(userName, false), ResourceAction.SUBSCRIPTION_DELETE, resourcePath)
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
        when(iamPolicyService.getIAMPolicy(eq(ResourceType.PROJECT), anyString()))
                .thenReturn(new RoleBindingNode("proj_a", ResourceType.PROJECT,
                        Map.of(userName, Set.of("project.writer", "subscription.reader")), 1
                ));
        when(iamPolicyService.getIAMPolicy(eq(ResourceType.SUBSCRIPTION), eq("proj_a:sub_a")))
                .thenReturn(new RoleBindingNode("proj_a:sub_a", ResourceType.SUBSCRIPTION,
                        Map.of(userName, Set.of("subscription.reader")), 1
                ));
        when(iamPolicyService.getIAMPolicy(eq(ResourceType.SUBSCRIPTION), eq("proj_a:sub_b")))
                .thenReturn(new RoleBindingNode("proj_a:sub_b", ResourceType.SUBSCRIPTION,
                        Map.of(userName, Set.of("subscription.admin")), 1
                ));

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(authorizationOptions)
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))
                .compose(t -> provider.isAuthorized(
                        testUser(userName, false), ResourceAction.SUBSCRIPTION_UPDATE, resourcePath)
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
        when(iamPolicyService.getIAMPolicy(eq(ResourceType.TOPIC), eq("proj_a:topic_a")))
                .thenReturn(new RoleBindingNode("proj_a:topic_a", ResourceType.TOPIC,
                        Map.of(userName, Set.of("topic.admin")), 1
                ));

        doReturn(iamPolicyService).when(provider).getAuthZService();

        provider.init(authorizationOptions)
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))
                .compose(t -> provider.isAuthorized(
                        testUser(userName, false), ResourceAction.TOPIC_UPDATE, resourcePath)
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

        provider.init(authorizationOptions)
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))
                .compose(t -> provider.isAuthorized(
                        testUser(userName, false), ResourceAction.TEAM_CREATE, resourcePath)
                )
                .onComplete(testContext.failing(t -> {
                    Assertions.assertInstanceOf(IllegalArgumentException.class, t);
                    checkpoint.flag();
                }));
    }
}
