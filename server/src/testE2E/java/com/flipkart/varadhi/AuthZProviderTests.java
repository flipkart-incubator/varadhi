package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.web.authz.DefaultAuthorizationProvider;
import com.flipkart.varadhi.web.spi.authz.AuthorizationOptions;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.entities.web.TopicResource;
import com.flipkart.varadhi.entities.auth.IamPolicyRequest;
import com.flipkart.varadhi.entities.auth.IamPolicyResponse;
import com.flipkart.varadhi.entities.auth.ResourceAction;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.flipkart.varadhi.entities.TestUser.testUser;

@ExtendWith (VertxExtension.class)
public class AuthZProviderTests extends E2EBase {

    public static Org oPublic;
    public static Team fkTeamRocket, fkTeamAsh;
    public static Project fkDefault;
    public static TopicResource fkTopic001;

    @TempDir
    public static Path tempDir;

    public static DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
    public static AuthorizationOptions authorizationOptions;

    public static MeterRegistry meterRegistry = new SimpleMeterRegistry();

    @BeforeAll
    public static void setup(VertxTestContext testContext) throws IOException, InterruptedException {
        Checkpoint checkpoint = testContext.checkpoint(1);

        oPublic = Org.of("public");
        fkTeamRocket = Team.of("team_rocket", oPublic.getName());
        fkTeamAsh = Team.of("team_ash", oPublic.getName());
        fkDefault = Project.of("default", "", fkTeamRocket.getName(), oPublic.getName());
        fkTopic001 = TopicResource.unGrouped(
            "topic001",
            fkDefault.getName(),
            null,
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            "test"
        );
        makeCreateRequest(getOrgsUri(), oPublic, 200);
        makeCreateRequest(getTeamsUri(oPublic.getName()), fkTeamRocket, 200);
        makeCreateRequest(getTeamsUri(oPublic.getName()), fkTeamAsh, 200);
        makeCreateRequest(getProjectCreateUri(), fkDefault, 200);

        // Add a small delay to allow the project cache to be updated
        try {
            Thread.sleep(500); // 500ms delay
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        makeCreateRequest(getTopicsUri(fkDefault), fkTopic001, 200);
        bootstrapRoleBindings();
        setupProvider(checkpoint);
    }

    @AfterAll
    public static void cleanup() {
        cleanupRoleBindings();
    }

    private static void setupProvider(Checkpoint checkpoint) throws IOException {
        String configContent = """
            ---
            superUsers: [ "thanos" ]

            metaStoreOptions:
              providerClassName: "com.flipkart.varadhi.db.ZookeeperProvider"
              configFile: "metastore.yml"

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
        Path configFile = tempDir.resolve("authorizationConfig.yml");
        Files.write(configFile, configContent.getBytes());

        authorizationOptions = new AuthorizationOptions();
        authorizationOptions.setConfigFile(configFile.toString());
        provider.init(c -> c, authorizationOptions, meterRegistry).onSuccess(t -> checkpoint.flag());
    }

    private static ConcurrentHashMap<String, Runnable> policyCleanupHandlers = new ConcurrentHashMap<>();

    private static String getIamPolicyUri(String resourceUri) {
        return String.join("/", VARADHI_BASE_URI, "v1", resourceUri, "policy");
    }

    private static void bootstrapRoleBindings() {
        setIamPolicy(getIamPolicyUri("orgs/public"), new IamPolicyRequest("abc", Set.of("team.admin")));
        setIamPolicy(getIamPolicyUri("orgs/public"), new IamPolicyRequest("xyz", Set.of("org.admin")));
        setIamPolicy(
            getIamPolicyUri("orgs/public/teams/team_rocket"),
            new IamPolicyRequest("team_user1", Set.of("team.admin"))
        );
        setIamPolicy(
            getIamPolicyUri("orgs/public/teams/team_ash"),
            new IamPolicyRequest("brock", Set.of("team.admin"))
        );
        setIamPolicy(getIamPolicyUri("projects/default"), new IamPolicyRequest("proj_user1", Set.of("project.read")));
        setIamPolicy(getIamPolicyUri("projects/default"), new IamPolicyRequest("proj_user2", Set.of("topic.read")));
        setIamPolicy(
            getIamPolicyUri("projects/default/topics/topic001"),
            new IamPolicyRequest("proj_user3", Set.of("topic.read"))
        );
    }

    private static void cleanupRoleBindings() {
        cleanupPolicies();
        cleanupOrgs(List.of(oPublic));
    }

    private static void registerPolicyCleanupHandler(String targetUrl) {
        policyCleanupHandlers.putIfAbsent(targetUrl, () -> deleteIamPolicy(getIamPolicyUri(targetUrl)));
    }

    private static void cleanupPolicies() {
        policyCleanupHandlers.forEach((k, v) -> v.run());
    }

    private static void setIamPolicy(String targetUrl, IamPolicyRequest entity) {
        Response response = makeHttpPutRequest(targetUrl, entity);
        Assertions.assertNotNull(response);
        Assertions.assertEquals(200, response.getStatus());
        registerPolicyCleanupHandler(targetUrl);
        response.readEntity(IamPolicyResponse.class);
    }

    private static void deleteIamPolicy(String targetUrl) {
        Response response = makeHttpDeleteRequest(targetUrl);
        Assertions.assertNotNull(response);
    }

    @Test
    public void testIsAuthorized_SuperUserAccess(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);

        // Super user should be authorized for any action on any resource
        provider.isAuthorized(
            testUser("thanos", false),
            ResourceAction.TOPIC_GET,
            "public/team_rocket/default/topic001"
        ).onComplete(testContext.succeeding(t -> {
            Assertions.assertTrue(t);
            checkpoint.flag();
        }));
    }

    @Test
    public void testIsAuthorized_UserNotAuthorizedOnResource(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);

        provider.isAuthorized(testUser("abc", false), ResourceAction.ORG_CREATE, "public")
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserAuthorisedOnResource(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);

        provider.isAuthorized(testUser("xyz", false), ResourceAction.ORG_CREATE, "public")
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserNoNodeRoles(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);

        provider.isAuthorized(testUser("xyz", false), ResourceAction.ORG_CREATE, "") // xyz has org.admin but not at root level
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserTopicAccess(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);

        provider.isAuthorized(
            testUser("proj_user3", false),
            ResourceAction.TOPIC_GET,
            "public/team_rocket/default/topic001"
        ) // checking if user role at the leaf node resolves
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserTopicAccess2(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);

        provider.isAuthorized(
            testUser("proj_user2", false),
            ResourceAction.TOPIC_GET,
            "public/team_rocket/default/topic001"
        )// checking if user role at the parent node resolves
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserTopicAccess3(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);

        provider.isAuthorized(testUser("abc", false), ResourceAction.TOPIC_GET, "public/team_rocket/default/topic001") // checking since abc is team.admin, they should be able to read the topic
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserTopicAccess4(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);

        provider.isAuthorized(
            testUser("team_user1", false),
            ResourceAction.TOPIC_GET,
            "public/team_rocket/default/topic001"
        ) // checking since team_user1 is team.admin, they should be able to read the topic
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserTopicAccess5(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);

        provider.isAuthorized(testUser("brock", false), ResourceAction.TOPIC_GET, "public/team_rocket/default/topic001") // brock is team admin for different team, should not be able to access
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserProjectAccess(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);
        provider.isAuthorized(testUser("proj_user2", false), ResourceAction.PROJECT_GET, "public/team_rocket/default") // proj_user2 only has topic read access, so should fail
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserProjectAccess2(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);
        provider.isAuthorized(testUser("proj_user2", false), ResourceAction.TOPIC_GET, "public/team_rocket/default") // proj_user2 only has topic read access, so should fail
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserProjectAccess3(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);
        provider.isAuthorized(testUser("proj_user1", false), ResourceAction.PROJECT_GET, "public/team_rocket/default") // proj_user1 is project.read so should work
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }
}
