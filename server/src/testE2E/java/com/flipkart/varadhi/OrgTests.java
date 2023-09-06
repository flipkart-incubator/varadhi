package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.Org;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class OrgTests extends E2EBase {

    Org org1;
    Org org2;


    @BeforeEach
    public void setup() {
        org1 = new Org("org10", 0);
        org2 = new Org("org12", 0);
    }

    @AfterEach
    public void cleanup() {
        cleanupOrgs(List.of(org1, org2));
    }

    @Test
    public void testOrgCreation() {
        Org org1Created = makeCreateRequest(getOrgsUri(), org1, 200);
        Assertions.assertEquals(org1, org1Created);
        makeCreateRequest(getOrgsUri(), org2, 200);
        makeCreateRequest(
                getOrgsUri(),
                org2,
                409,
                String.format("Org(%s) already exists.", org2.getName()), true
        );
        Org orgGet = makeGetRequest(getOrgUri(org1), Org.class, 200);
        Assertions.assertEquals(org1, orgGet);
        List<Org> orgList1 = getOrgs(makeListRequest(getOrgsUri(), 200));
        Assertions.assertTrue(orgList1.contains(org1));
        Assertions.assertTrue(orgList1.contains(org2));
        makeDeleteRequest(getOrgUri(org1), 200);
        makeGetRequest(getOrgUri(org1), 404, String.format("Org(%s) not found.", org1.getName()), true);
        List<Org> orgList2 = getOrgs(makeListRequest(getOrgsUri(), 200));
        Assertions.assertTrue(!orgList2.contains(org1));
        Assertions.assertTrue(orgList2.contains(org2));
    }

}
