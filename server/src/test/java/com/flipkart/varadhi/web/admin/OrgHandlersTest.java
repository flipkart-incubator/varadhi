package com.flipkart.varadhi.web.admin;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.services.OrgService;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.web.v1.admin.OrgHandlers;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class OrgHandlersTest extends WebTestBase {
    OrgHandlers orgHandlers;
    OrgService orgService;
    String orgsPath = "/orgs";

    @BeforeEach
    public void PreTest() throws InterruptedException {
        super.setUp();
        orgService = mock(OrgService.class);
        orgHandlers = new OrgHandlers(orgService);

        Route routeCreate = router.post("/orgs").handler(bodyHandler).handler(ctx -> {
            orgHandlers.setOrg(ctx);
            ctx.next();
        }).handler(wrapBlocking(orgHandlers::create));
        setupFailureHandler(routeCreate);
        Route routeGet = router.get("/orgs/:org").handler(wrapBlocking(orgHandlers::get));
        setupFailureHandler(routeGet);
        Route routeDelete = router.delete("/orgs/:org").handler(wrapBlocking(orgHandlers::delete));
        setupFailureHandler(routeDelete);
        Route routeList = router.get("/orgs").handler(wrapBlocking(orgHandlers::list));
        setupFailureHandler(routeList);
    }


    private String getOrgUrl(String name) {
        return String.join("/", orgsPath, name);
    }

    @AfterEach
    public void PostTest() throws InterruptedException {
        super.tearDown();
    }

    @Test
    public void testOrgCreation() throws InterruptedException {

        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, orgsPath);
        Org org1 = Org.of("name1");
        doReturn(org1).when(orgService).createOrg(eq(org1));
        Org org1Created = sendRequestWithEntity(request, org1, Org.class);
        Assertions.assertEquals(org1, org1Created);
        verify(orgService, times(1)).createOrg(eq(org1));

        String duplicateOrgError = String.format("Org(%s) already exists. Org is globally unique.", org1.getName());
        doThrow(new DuplicateResourceException(duplicateOrgError)).when(orgService).createOrg(org1);
        ErrorResponse response = sendRequestWithEntity(request, org1, 409, duplicateOrgError, ErrorResponse.class);
        Assertions.assertEquals(duplicateOrgError, response.reason());

        String someInternalError = "Some random error";
        doThrow(new MetaStoreException(someInternalError)).when(orgService).createOrg(org1);
        response = sendRequestWithEntity(request, org1, 500, someInternalError, ErrorResponse.class);
        Assertions.assertEquals(someInternalError, response.reason());
    }

    @Test
    public void testOrgCreationInvalidNames() throws InterruptedException {
        sendInvalidName("");
        sendInvalidName(null);
        sendInvalidName("ab");
        sendInvalidName("_startwithunderscore");
        sendInvalidName("-startwithhyphen");
        sendInvalidName("endswith-");
        sendInvalidName("endswith_");
        sendInvalidName("has.specialchar");
    }

    private void sendInvalidName(String name) throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, orgsPath);
        String orgNameErr = "Invalid Org name. Check naming constraints.";
        Org org = Org.of(name);
        ErrorResponse response = sendRequestWithEntity(request, org, 400, orgNameErr, ErrorResponse.class);
        Assertions.assertEquals(orgNameErr, response.reason());
    }

    @Test
    public void testOrgGet() throws InterruptedException {
        Org org1 = Org.of("name1");

        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, getOrgUrl(org1.getName()));
        doReturn(org1).when(orgService).getOrg(org1.getName());

        Org org1Get = sendRequestWithoutPayload(request, Org.class);
        Assertions.assertEquals(org1, org1Get);
        verify(orgService, times(1)).getOrg(org1.getName());

        String notFoundError = String.format("Org(%s) not found.", org1.getName());
        doThrow(new ResourceNotFoundException(notFoundError)).when(orgService).getOrg(org1.getName());
        sendRequestWithoutPayload(request, 404, notFoundError);
    }

    @Test
    public void testOrgList() throws Exception {
        List<Org> orgList = List.of(Org.of("org_1"), Org.of("org_2"));
        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, orgsPath);
        doReturn(orgList).when(orgService).getOrgs();

        List<Org> orgListObtained = listOrganisations(request);
        Assertions.assertEquals(orgList.size(), orgListObtained.size());
        Assertions.assertArrayEquals(orgList.toArray(), orgListObtained.toArray());
        verify(orgService, times(1)).getOrgs();
    }

    List<Org> listOrganisations(HttpRequest<Buffer> request) throws Exception {
        HttpResponse<Buffer> response = sendRequest(request, null);
        return jsonDeserialize(response.bodyAsString(), List.class, Org.class);
    }

    @Test
    public void testOrgDelete() throws InterruptedException {
        Org org1 = Org.of("name1");

        HttpRequest<Buffer> request = createRequest(HttpMethod.DELETE, getOrgUrl(org1.getName()));
        doNothing().when(orgService).deleteOrg(org1.getName());
        sendRequestWithoutPayload(request, null);
        verify(orgService, times(1)).deleteOrg(org1.getName());

        String notFoundError = String.format("Org(%s) not found.", org1.getName());
        doThrow(new ResourceNotFoundException(notFoundError)).when(orgService).deleteOrg(org1.getName());
        sendRequestWithoutPayload(request, 404, notFoundError);

        String invalidOpError = String.format("Can not delete Org(%s) as it has associated Team(s).", org1.getName());
        doThrow(new InvalidOperationForResourceException(invalidOpError)).when(orgService).deleteOrg(org1.getName());
        sendRequestWithoutPayload(request, 409, invalidOpError);
    }

}
