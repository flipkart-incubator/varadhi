package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import com.flipkart.varadhi.utils.YamlLoader;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;

import java.util.HashSet;
import java.util.List;

import static org.apache.commons.text.CharacterPredicates.DIGITS;
import static org.apache.commons.text.CharacterPredicates.LETTERS;

public class PulsarTestBase {
    public static final String TENANT = "testTenant";
    public static final String NAMESPACE = "testNamespace";
    static String pulsarConfigFileName = "pulsartestconfig.yml";
    RandomStringGenerator stringGenerator;

    static PulsarConfig pulsarConfig;
    ClientProvider clientProvider;

    static void loadPulsarConfig() {
        String filePath = PulsarTopicServiceTest.class.getClassLoader().getResource(pulsarConfigFileName).getFile();
        pulsarConfig = YamlLoader.loadConfig(filePath, PulsarConfig.class);
    }

    void init() throws PulsarAdminException {
        clientProvider = new ClientProvider(pulsarConfig);
        stringGenerator = new RandomStringGenerator.Builder().withinRange('0', 'z').filteredBy(DIGITS, LETTERS).build();
        ensureTenantExists();
        ensureNameSpaceExists();
    }

    String getNamespace() {
        return String.format("%s/%s", TENANT, NAMESPACE);
    }

    String getRandomTopicFQDN() {
        String name = stringGenerator.generate(20);
        return getTopicFQDN(name);
    }

    String getTopicFQDN(String name) {
        return String.format("persistent://%s/%s", getNamespace(), name);
    }

    public void ensureTenantExists() throws PulsarAdminException {
        List<String> tenants = clientProvider.getAdminClient().tenants().getTenants();
        if (!tenants.contains(TENANT)) {
            List<String> clusters = clientProvider.getAdminClient().clusters().getClusters();
            TenantInfo tenantInfo = TenantInfoImpl.builder().allowedClusters(new HashSet<>(clusters)).build();
            clientProvider.getAdminClient().tenants().createTenant(TENANT, tenantInfo);
        }
    }

    public void ensureNameSpaceExists() throws PulsarAdminException {
        String namespace = getNamespace();
        List<String> namespaces = clientProvider.getAdminClient().namespaces().getNamespaces(TENANT);
        if (!namespaces.contains(namespace)) {
            clientProvider.getAdminClient().namespaces().createNamespace(namespace);
        }
    }
}
