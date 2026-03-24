package com.flipkart.varadhi.web.v1.admin;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.core.RegionService;
import com.flipkart.varadhi.entities.Region;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.RegionStatus;
import com.flipkart.varadhi.entities.web.ErrorResponse;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.web.WebTestBase;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.client.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RegionHandlersTest extends WebTestBase {

    private static final String REGIONS_PATH = "/regions";

    RegionHandlers regionHandlers;
    RegionService regionService;

    @BeforeEach
    public void preTest() throws InterruptedException {
        super.setUp();
        regionService = mock(RegionService.class);
        regionHandlers = new RegionHandlers(regionService);

        Route routeCreate = router.post(REGIONS_PATH).handler(bodyHandler).handler(ctx -> {
            regionHandlers.setRegion(ctx);
            ctx.next();
        }).handler(wrapBlocking(regionHandlers::create));
        setupFailureHandler(routeCreate);

        Route routeGet = router.get(REGIONS_PATH + "/:region").handler(wrapBlocking(regionHandlers::get));
        setupFailureHandler(routeGet);

        Route routeDelete = router.delete(REGIONS_PATH + "/:region").handler(wrapBlocking(regionHandlers::delete));
        setupFailureHandler(routeDelete);

        Route routeList = router.get(REGIONS_PATH).handler(wrapBlocking(regionHandlers::list));
        setupFailureHandler(routeList);
    }

    @AfterEach
    public void postTest() throws InterruptedException {
        super.tearDown();
    }

    private String regionUrl(String name) {
        return String.join("/", REGIONS_PATH, name);
    }

    @Test
    public void testRegionCreate() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, REGIONS_PATH);
        Region region = Region.of(new RegionName("valid-region"), RegionStatus.AVAILABLE);
        doReturn(region).when(regionService).createRegion(eq(region));

        Region created = sendRequestWithEntity(request, region, WebTestBase.c(Region.class));
        Assertions.assertEquals(region, created);
        verify(regionService, times(1)).createRegion(eq(region));

        String duplicateMsg = "Region(valid-region) already exists.";
        doThrow(new DuplicateResourceException(duplicateMsg)).when(regionService).createRegion(eq(region));
        ErrorResponse response = sendRequestWithEntity(
            request,
            region,
            409,
            duplicateMsg,
            WebTestBase.c(ErrorResponse.class)
        );
        Assertions.assertEquals(duplicateMsg, response.reason());

        String internal = "ZK write failed";
        doThrow(new MetaStoreException(internal)).when(regionService).createRegion(eq(region));
        response = sendRequestWithEntity(request, region, 500, internal, WebTestBase.c(ErrorResponse.class));
        Assertions.assertEquals(internal, response.reason());
    }

    @Test
    public void testRegionCreateInvalidNames() throws InterruptedException {
        sendInvalidName("");
        sendInvalidName("ab");
        sendInvalidName("_badstart");
        sendInvalidName("-badstart");
        sendInvalidName("badend-");
        sendInvalidName("badend_");
        sendInvalidName("has.dot");
    }

    private void sendInvalidName(String name) throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, REGIONS_PATH);
        String err = "Invalid Region name. Check naming constraints.";
        Region region = new Region(name, Region.INITIAL_VERSION, RegionStatus.AVAILABLE);
        ErrorResponse response = sendRequestWithEntity(
            request,
            region,
            400,
            err,
            WebTestBase.c(ErrorResponse.class)
        );
        Assertions.assertEquals(err, response.reason());
    }

    @Test
    public void testRegionGet() throws InterruptedException {
        Region region = Region.of(new RegionName("my-region"), RegionStatus.CONSUME_UNAVAILABLE);
        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, regionUrl(region.getName()));
        doReturn(region).when(regionService).getRegion(region.getName());

        Region loaded = sendRequestWithoutPayload(request, WebTestBase.c(Region.class));
        Assertions.assertEquals(region, loaded);
        verify(regionService, times(1)).getRegion(region.getName());

        String notFound = "Region(my-region) not found.";
        doThrow(new ResourceNotFoundException(notFound)).when(regionService).getRegion(region.getName());
        sendRequestWithoutPayload(request, 404, notFound);
    }

    @Test
    public void testRegionList() throws Exception {
        List<Region> regions = List.of(
            Region.of(new RegionName("r-one"), RegionStatus.AVAILABLE),
            Region.of(new RegionName("r-two"), RegionStatus.UNAVAILABLE)
        );
        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, REGIONS_PATH);
        doReturn(regions).when(regionService).getRegions();

        List<Region> obtained = sendRequestWithoutPayload(request, WebTestBase.t(new TypeReference<>() {
        }));
        Assertions.assertEquals(regions.size(), obtained.size());
        Assertions.assertArrayEquals(regions.toArray(), obtained.toArray());
        verify(regionService, times(1)).getRegions();
    }

    @Test
    public void testRegionDelete() throws InterruptedException {
        String name = "to-delete";
        HttpRequest<Buffer> request = createRequest(HttpMethod.DELETE, regionUrl(name));
        doNothing().when(regionService).deleteRegion(name);
        sendRequestWithoutPayload(request, HTTP_NO_CONTENT);
        verify(regionService, times(1)).deleteRegion(name);

        String notFound = String.format("Region(%s) not found.", name);
        doThrow(new ResourceNotFoundException(notFound)).when(regionService).deleteRegion(name);
        sendRequestWithoutPayload(request, 404, notFound);
    }
}
