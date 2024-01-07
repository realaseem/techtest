package com.db.dataplatform.techtest.client.component.impl;

import com.db.dataplatform.techtest.client.api.model.DataEnvelope;
import com.db.dataplatform.techtest.client.component.Client;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Client code does not require any test coverage
 */

@Service
@Slf4j
@RequiredArgsConstructor
public class ClientImpl implements Client {

    private final RestTemplate restTemplate;

    public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
    public static final UriTemplate URI_GETDATA = new UriTemplate("http://localhost:8090/dataserver/data/{blockType}");
    public static final UriTemplate URI_PATCHDATA = new UriTemplate("http://localhost:8090/dataserver/update/{name}/{newBlockType}");

    @Override
    public void pushData(DataEnvelope dataEnvelope) {
        log.info("Pushing data {} to {}", dataEnvelope.getDataHeader().getName(), URI_PUSHDATA);
        ResponseEntity<Boolean> checksum = restTemplate.postForEntity(URI_PUSHDATA, dataEnvelope, Boolean.class);
        log.info("Checksum matched for request: {}", checksum.getBody());
    }

    @Override
    public List<DataEnvelope> getData(String blockType) {
        log.info("Query for data with header block type {}", blockType);
        ResponseEntity<List<DataEnvelope>> exchange = restTemplate.exchange(URI_GETDATA.toString(), HttpMethod.GET, null,
                new ParameterizedTypeReference<List<DataEnvelope>>() {}, blockType);
        return exchange.getBody();
    }

    @Override
    public boolean updateData(String blockName, String newBlockType) {
        log.info("Updating blocktype to {} for block with name {}", newBlockType, blockName);
        Boolean response = restTemplate.patchForObject(URI_PATCHDATA.toString(), null, Boolean.class, blockName, newBlockType);
        log.info("Patch request executed successfully {}", response);
        return Boolean.TRUE.equals(response);
    }


}
