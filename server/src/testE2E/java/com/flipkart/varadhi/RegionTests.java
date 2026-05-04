package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.Region;
import com.flipkart.varadhi.entities.RegionStatus;
import com.flipkart.varadhi.entities.web.RegionCreateRequest;
import com.flipkart.varadhi.entities.web.RegionStatusUpdateRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RegionTests extends E2EBase {

    RegionCreateRequest region1Req;
    RegionCreateRequest region2Req;

    @BeforeEach
    public void setup() {
        region1Req = new RegionCreateRequest("dc1", RegionStatus.AVAILABLE);
        region2Req = new RegionCreateRequest("dc2", RegionStatus.UNAVAILABLE);
    }

    @AfterEach
    public void cleanup() {
        cleanupRegions(List.of(region1Req.name(), region2Req.name()));
    }

    @Test
    public void testRegionCRUD() {
        // Create two regions and verify the returned entity matches the request
        Region created1 = makeCreateRequest(getRegionsUri(), region1Req, 200, Region.class);
        Assertions.assertEquals(region1Req.name(), created1.getName());
        Assertions.assertEquals(region1Req.status(), created1.getStatus());

        Region created2 = makeCreateRequest(getRegionsUri(), region2Req, 200, Region.class);
        Assertions.assertEquals(region2Req.name(), created2.getName());
        Assertions.assertEquals(region2Req.status(), created2.getStatus());

        // Duplicate create must return 409
        makeCreateRequest(
            getRegionsUri(),
            region1Req,
            409,
            String.format("Region(%s) already exists.", region1Req.name()),
            true
        );

        // Get by name
        Region fetched = makeGetRequest(getRegionUri(region1Req.name()), Region.class, 200);
        Assertions.assertEquals(region1Req.name(), fetched.getName());
        Assertions.assertEquals(region1Req.status(), fetched.getStatus());

        // List must contain both regions
        List<Region> allRegions = getRegions(makeListRequest(getRegionsUri(), 200));
        Assertions.assertTrue(allRegions.stream().anyMatch(r -> r.getName().equals(region1Req.name())));
        Assertions.assertTrue(allRegions.stream().anyMatch(r -> r.getName().equals(region2Req.name())));

        // Patch status: flip region1 to UNAVAILABLE
        RegionStatusUpdateRequest patchReq = new RegionStatusUpdateRequest(RegionStatus.UNAVAILABLE);
        Region patched = makePatchRequest(getRegionUri(region1Req.name()), patchReq, 200, Region.class);
        Assertions.assertEquals(region1Req.name(), patched.getName());
        Assertions.assertEquals(RegionStatus.UNAVAILABLE, patched.getStatus());

        // Confirm patched status is persisted via GET
        Region afterPatch = makeGetRequest(getRegionUri(region1Req.name()), Region.class, 200);
        Assertions.assertEquals(RegionStatus.UNAVAILABLE, afterPatch.getStatus());

        // Delete region1
        makeDeleteRequest(getRegionUri(region1Req.name()), 204);

        // GET after delete must return 404
        makeGetRequest(
            getRegionUri(region1Req.name()),
            404,
            String.format("Region(%s) not found.", region1Req.name()),
            true
        );

        // List after delete must not contain region1 but still contain region2
        List<Region> regionsAfterDelete = getRegions(makeListRequest(getRegionsUri(), 200));
        Assertions.assertFalse(regionsAfterDelete.stream().anyMatch(r -> r.getName().equals(region1Req.name())));
        Assertions.assertTrue(regionsAfterDelete.stream().anyMatch(r -> r.getName().equals(region2Req.name())));
    }

    @Test
    public void testRegionInvalidOps() {
        String missingRegion = "nonexistent-region";

        // GET non-existent region
        makeGetRequest(
            getRegionUri(missingRegion),
            404,
            String.format("Region(%s) not found.", missingRegion),
            true
        );

        // DELETE non-existent region
        makeDeleteRequest(
            getRegionUri(missingRegion),
            404,
            String.format("Region(%s) not found.", missingRegion),
            true
        );

        // PATCH non-existent region
        RegionStatusUpdateRequest patchReq = new RegionStatusUpdateRequest(RegionStatus.UNAVAILABLE);
        makePatchRequest(
            getRegionUri(missingRegion),
            patchReq,
            404,
            String.format("Region(%s) not found.", missingRegion),
            true
        );

        // Create region1 then verify a second create with same name fails
        makeCreateRequest(getRegionsUri(), region1Req, 200, Region.class);
        makeCreateRequest(
            getRegionsUri(),
            region1Req,
            409,
            String.format("Region(%s) already exists.", region1Req.name()),
            true
        );
    }
}
