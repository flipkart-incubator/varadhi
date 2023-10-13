package com.flipkart.varadhi.auth;

import com.flipkart.varadhi.spi.services.MessagingStackOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Assertions;
import org.mockito.Spy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.mockito.Mockito.spy;

public class DefaultAuthorizationProviderTest {
    
    @TempDir
    Path tempDir;

    private AuthorizationOptions authorizationOptions;

    private DefaultAuthorizationProvider defaultAuthorizationProvider;
    
    @BeforeEach
    public void setUp() throws IOException {
        String configContent = "---\nroles:\n  org.admin:\n    - ORG_CREATE\n    - ORG_UPDATE\n    - ORG_GET\n    - ORG_DELETE\n  team.admin:\n    - TEAM_CREATE\n    - TEAM_GET\n    - TEAM_UPDATE\nroleBindings:\n  flipkart:\n    aayush.gupta:\n      - org.admin\n      - team.admin\n    abc:\n      - team.admin\n    xyz:\n      - org.admin\n";
        Path configFile = tempDir.resolve("authorizationConfig.yaml");
        Files.write(configFile, configContent.getBytes());

        authorizationOptions = new AuthorizationOptions();
        authorizationOptions.setUseDefaultProvider(true);
        authorizationOptions.setConfigFile(configFile.toString());

        defaultAuthorizationProvider = spy(new DefaultAuthorizationProvider());
    }

    @Test
    public void testInit() {
        defaultAuthorizationProvider.init(authorizationOptions);
    }
}
