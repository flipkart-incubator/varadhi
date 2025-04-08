package com.flipkart.varadhi.web.admin;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.common.utils.JsonMapper;
import com.flipkart.varadhi.entities.filters.Condition;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.entities.filters.StringConditions;
import com.flipkart.varadhi.services.OrgService;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.web.v1.admin.OrgFilterHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.client.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class OrgFilterHandlerTest extends WebTestBase {
    OrgFilterHandler orgFilterHandler;
    OrgService orgService;
    String basePath = "/v1/orgs";

    @BeforeEach
    public void setUpTest() throws InterruptedException {
        super.setUp();
        orgService = mock(OrgService.class);
        orgFilterHandler = new OrgFilterHandler(orgService);

        // Register filter endpoints under /v1/orgs/:org/filters
        Route routeGetFilters = router.get("/v1/orgs/:org/filters").handler(wrapBlocking(orgFilterHandler::getAll));
        setupFailureHandler(routeGetFilters);
        Route routeGetFilterByName = router.get("/v1/orgs/:org/filters/:orgFilterName")
                                           .handler(wrapBlocking(orgFilterHandler::get));
        setupFailureHandler(routeGetFilterByName);
        Route routeCreateFilter = router.post("/v1/orgs/:org/filters").handler(bodyHandler).handler(ctx -> {
            orgFilterHandler.setNamedFilter(ctx);
            ctx.next();
        }).handler(wrapBlocking(orgFilterHandler::create));
        setupFailureHandler(routeCreateFilter);
        Route routeUpdateFilter = router.put("/v1/orgs/:org/filters/:orgFilterName")
                                        .handler(bodyHandler)
                                        .handler(ctx -> {
                                            orgFilterHandler.setNamedFilter(ctx);
                                            ctx.next();
                                        })
                                        .handler(wrapBlocking(orgFilterHandler::update));
        setupFailureHandler(routeUpdateFilter);
        Route routeCheckExists = router.get("/v1/orgs/:org/filters/:orgFilterName/exists")
                                       .handler(wrapBlocking(orgFilterHandler::exists));
        setupFailureHandler(routeCheckExists);
        Route routeDeleteFilter = router.delete("/v1/orgs/:org/filters")
                                        .handler(wrapBlocking(orgFilterHandler::delete));
        setupFailureHandler(routeDeleteFilter);
    }

    private String getFilterBaseUrl(String org) {
        return String.join("/", basePath, org, "filters");
    }

    private String getFilterByNameUrl(String org, String filterName) {
        return String.join("/", basePath, org, "filters", filterName);
    }

    private String getFilterExistsUrl(String org, String filterName) {
        return String.join("/", basePath, org, "filters", filterName, "exists");
    }

    @AfterEach
    public void tearDownTest() throws InterruptedException {
        super.tearDown();
    }

    @Test
    public void testGetAll() throws Exception {
        String orgName = "org1";
        Map<String, Condition> conditionMap = new HashMap<>();
        conditionMap.put("filterA", new StringConditions.ExistsCondition("X_abc"));
        conditionMap.put("nameGroup.filterName", new StringConditions.ExistsCondition("X_abc"));
        OrgFilters orgFilter = new OrgFilters(0, conditionMap);
        // Assume filters are set in the service
        when(orgService.getAllFilters(eq(orgName))).thenReturn(orgFilter);

        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, getFilterBaseUrl(orgName));
        OrgFilters obtainedFilters = sendRequestWithoutPayload(request, OrgFilters.class);
        Assertions.assertEquals(orgFilter, obtainedFilters);
        verify(orgService, times(1)).getAllFilters(eq(orgName));

        // Test not found scenario
        when(orgService.getAllFilters(eq(orgName))).thenThrow(
            new ResourceNotFoundException("Org(" + orgName + ") filter not found.")
        );
        sendRequestWithoutPayload(request, 404, "Org(" + orgName + ") filter not found.");
    }

    @Test
    public void testGet() throws Exception {
        String orgName = "org1";
        String filterName = "myFilter";
        Condition condition = new StringConditions.ContainsCondition("test", "x_abc");
        when(orgService.getFilter(eq(orgName), eq(filterName))).thenReturn(condition);

        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, getFilterByNameUrl(orgName, filterName));
        Condition obtained = sendRequestWithoutPayload(request, Condition.class);
        Assertions.assertEquals(condition, obtained);
        verify(orgService, times(1)).getFilter(eq(orgName), eq(filterName));

        // Negative scenario: filter not found
        String errorMsg = "Filter not found.";
        when(orgService.getFilter(eq(orgName), eq(filterName))).thenThrow(new ResourceNotFoundException(errorMsg));
        sendRequestWithoutPayload(request, 404, errorMsg);
    }

    @Test
    public void testCreate() throws Exception {
        String orgName = "org1";
        Map<String, Condition> conditionMap = new HashMap<>();
        conditionMap.put("filterA", new StringConditions.ExistsCondition("X_abc"));
        conditionMap.put("nameGroup.filterName", new StringConditions.ExistsCondition("X_abc"));
        OrgFilters inputFilters = new OrgFilters(0, conditionMap);
        OrgFilters createdFilters = new OrgFilters(0, conditionMap);


        when(orgService.createFilter(eq(orgName), eq(inputFilters))).thenReturn(createdFilters);

        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, getFilterBaseUrl(orgName));
        OrgFilters obtained = sendRequestWithEntity(request, inputFilters, OrgFilters.class);
        Assertions.assertEquals(createdFilters, obtained);
        verify(orgService, times(1)).createFilter(eq(orgName), eq(inputFilters));

        // Error case: creation fails
        String errorMsg = "Failed to create filter.";
        when(orgService.createFilter(eq(orgName), eq(inputFilters))).thenThrow(new ResourceNotFoundException(errorMsg));
        ErrorResponse response = sendRequestWithEntity(request, inputFilters, 404, errorMsg, ErrorResponse.class);
        Assertions.assertEquals(errorMsg, response.reason());
    }

    @Test
    public void testUpdate() throws Exception {
        String orgName = "org1";
        String filterName = "nameGroup.filterName";
        String jsonUpdate = """
            {
                "version": 0,
                "filters": {
                    "filterA": {
                        "op": "exists",
                        "key": "X_abc"
                    },
                    "nameGroup.filterName": {
                        "op": "exists",
                        "key": "X_abc"
                    }
                }
            }
            """;
        OrgFilters inputFilters = JsonMapper.getMapper().readValue(jsonUpdate, OrgFilters.class);

        // assume update does not return any entity
        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, getFilterByNameUrl(orgName, filterName));
        sendRequestWithEntity(request, inputFilters, null);
        verify(orgService, times(1)).updateFilter(eq(orgName), eq(filterName), eq(inputFilters));
    }

    @Test
    public void testCheckIfNamedFilterExists() throws Exception {
        String orgName = "org1";
        String filterName = "myFilter";
        when(orgService.filterExists(eq(orgName), eq(filterName))).thenReturn(true);

        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, getFilterExistsUrl(orgName, filterName));
        Boolean exists = sendRequestWithoutPayload(request, Boolean.class);
        Assertions.assertTrue(exists);
        verify(orgService, times(1)).filterExists(eq(orgName), eq(filterName));

        // Negative scenario
        when(orgService.filterExists(eq(orgName), eq(filterName))).thenReturn(false);
        exists = sendRequestWithoutPayload(request, Boolean.class);
        Assertions.assertFalse(exists);
    }

    @Test
    public void testDeleteFilter() throws Exception {
        String orgName = "org1";
        HttpRequest<Buffer> request = createRequest(HttpMethod.DELETE, getFilterBaseUrl(orgName));
        doNothing().when(orgService).deleteFilter(eq(orgName));
        sendRequestWithoutPayload(request, null);
        verify(orgService, times(1)).deleteFilter(eq(orgName));

        // Negative case: deletion for non-existent filter
        String errorMsg = "Org(" + orgName + ") filter not found.";
        doThrow(new ResourceNotFoundException(errorMsg)).when(orgService).deleteFilter(eq(orgName));
        sendRequestWithoutPayload(request, 404, errorMsg);
    }
}
