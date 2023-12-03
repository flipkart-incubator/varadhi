package com.flipkart.varadhi;

import com.flipkart.varadhi.auth.DefaultAuthorizationProvider;
import com.flipkart.varadhi.auth.RoleBindingNode;
import com.flipkart.varadhi.config.AuthorizationOptions;
import com.flipkart.varadhi.entities.*;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static com.flipkart.varadhi.Constants.INITIAL_VERSION;
import static com.flipkart.varadhi.entities.TestUser.testUser;

@ExtendWith(VertxExtension.class)
public class DefaultAuthZProviderTests extends E2EBase {

    public static Org oPublic;
    public static Team fkTeamRocket, fkTeamAsh;
    public static Project fkDefault;
    public static TopicResource fkTopic001;

    @TempDir
    public static Path tempDir;

    public static DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
    public static AuthorizationOptions authorizationOptions;

    @BeforeAll
    public static void setup(VertxTestContext testContext) throws IOException, InterruptedException {
        Checkpoint checkpoint = testContext.checkpoint(1);

        oPublic = new Org("public", 0);
        fkTeamRocket = new Team("team_rocket", 0, oPublic.getName());
        fkTeamAsh = new Team("team_ash", 0, oPublic.getName());
        fkDefault = new Project("default", 0, "", fkTeamRocket.getName(), oPublic.getName());
        fkTopic001 = new TopicResource("topic001", INITIAL_VERSION, fkDefault.getName(), false, null);
        makeCreateRequest(getOrgsUri(), oPublic, 200);
        makeCreateRequest(getTeamsUri(oPublic.getName()), fkTeamRocket, 200);
        makeCreateRequest(getTeamsUri(oPublic.getName()), fkTeamAsh, 200);
        makeCreateRequest(getProjectCreateUri(), fkDefault, 200);
        makeCreateRequest(getTopicsUri(fkDefault), fkTopic001, 200);
        bootstrapRoleBindings();
        setupProvider(checkpoint);
    }

    private static void setupProvider(Checkpoint checkpoint) throws IOException, InterruptedException {
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
        provider.init(Vertx.vertx(), authorizationOptions).onSuccess(t -> checkpoint.flag());
    }

    @AfterAll
    public static void cleanup() {
        cleanupRoleBindings();
    }

    private static void cleanupRoleBindings() {
        cleanupOrgs(List.of(oPublic));
        var allNodes = getRoleBindings(makeHttpGetRequest(getRoleBindingsUri()));
        allNodes.forEach(node -> makeDeleteRequest(getRoleBindingsUri(node.getResourceId()), 200));
    }

    private static List<RoleBindingNode> getRoleBindings(Response response) {
        return response.readEntity(new GenericType<>() {
        });
    }

    private static String getRoleBindingsUri() {
        return String.format("%s/v1/authz/bindings", VaradhiBaseUri);
    }

    private static String getRoleBindingsUri(String resourceId) {
        return String.join("/", getRoleBindingsUri(), resourceId);
    }

    private static void bootstrapRoleBindings() {
        makeRoleAssignmentUpdate(
                getRoleBindingsUri(),
                new RoleAssignmentUpdate("public", ResourceType.ORG, "abc", Set.of("team.admin"))
        );
        makeRoleAssignmentUpdate(
                getRoleBindingsUri(),
                new RoleAssignmentUpdate("public", ResourceType.ORG, "xyz", Set.of("org.admin"))
        );
        makeRoleAssignmentUpdate(
                getRoleBindingsUri(),
                new RoleAssignmentUpdate("public:team_rocket", ResourceType.TEAM, "team_user1", Set.of("team.admin"))
        );
        makeRoleAssignmentUpdate(
                getRoleBindingsUri(),
                new RoleAssignmentUpdate("public:team_ash", ResourceType.TEAM, "brock", Set.of("team.admin"))
        );
        makeRoleAssignmentUpdate(
                getRoleBindingsUri(),
                new RoleAssignmentUpdate("default", ResourceType.PROJECT, "proj_user1", Set.of("project.read"))
        );
        makeRoleAssignmentUpdate(
                getRoleBindingsUri(),
                new RoleAssignmentUpdate("default", ResourceType.PROJECT, "proj_user2", Set.of("topic.read"))
        );
        makeRoleAssignmentUpdate(
                getRoleBindingsUri(),
                new RoleAssignmentUpdate("default:topic001", ResourceType.TOPIC, "proj_user3", Set.of("topic.read"))
        );
    }

    private static void makeRoleAssignmentUpdate(String targetUrl, RoleAssignmentUpdate entity) {
        Response response = makeHttpPutRequest(targetUrl, entity);
        Assertions.assertNotNull(response);
        Assertions.assertEquals(200, response.getStatus());
        response.readEntity(RoleBindingNode.class);
    }

    @Test
    public void testIsAuthorized_UserNotAuthorizedOnResource(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);

        provider
                .isAuthorized(testUser("abc", false),
                        ResourceAction.ORG_CREATE, "public"
                )
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserAuthorisedOnResource(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);

        provider
                .isAuthorized(testUser("xyz", false),
                        ResourceAction.ORG_CREATE, "public"
                )
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserNoNodeRoles(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);

        provider
                .isAuthorized(testUser("xyz", false),
                        ResourceAction.ORG_CREATE, ""
                ) // xyz has org.admin but not at root level
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserTopicAccess(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(5);

        provider
                .isAuthorized(testUser("proj_user3", false),
                        ResourceAction.TOPIC_GET, "public/team_rocket/default/topic001"
                ) // checking if user role at the leaf node resolves
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))

                .compose(t -> provider.isAuthorized(testUser("proj_user2", false),
                        ResourceAction.TOPIC_GET, "public/team_rocket/default/topic001"
                )) // checking if user role at the parent node resolves
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))

                .compose(t -> provider.isAuthorized(testUser("abc", false),
                        ResourceAction.TOPIC_GET, "public/team_rocket/default/topic001"
                )) // checking since abc is team.admin, they should be able to read the topic
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))

                .compose(t -> provider.isAuthorized(testUser("team_user1", false),
                        ResourceAction.TOPIC_GET, "public/team_rocket/default/topic001"
                )) // checking since team_user1 is team.admin, they should be able to read the topic
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }))

                .compose(t -> provider.isAuthorized(testUser("brock", false),
                        ResourceAction.TOPIC_GET, "public/team_rocket/default/topic001"
                )) // brock is team admin for different team, should not be able to access
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserProjectAccess(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);
        provider
                .isAuthorized(testUser("proj_user2", false),
                        ResourceAction.PROJECT_GET, "public/team_rocket/default"
                ) // proj_user2 only has topic read access, so should fail
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertFalse(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserProjectAccess2(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);
        provider
                .isAuthorized(testUser("proj_user2", false),
                        ResourceAction.TOPIC_GET, "public/team_rocket/default"
                ) // proj_user2 only has topic read access, so should fail
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }

    @Test
    public void testIsAuthorized_UserProjectAccess3(VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(1);
        provider
                .isAuthorized(testUser("proj_user1", false),
                        ResourceAction.PROJECT_GET, "public/team_rocket/default"
                ) // proj_user1 is project.read so should work
                .onComplete(testContext.succeeding(t -> {
                    Assertions.assertTrue(t);
                    checkpoint.flag();
                }));
    }
}
