package com.db.dataplatform.techtest.server.api.controller;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.RetryException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Component
@RequiredArgsConstructor
public class DataLakePush {

    private static final String HADOOP_URL = "http://localhost:8090/hadoopserver/pushbigdata";

    private final ObjectMapper objectMapper;
    private final RestTemplate restTemplate;

    @Retryable(include = RetryException.class, backoff = @Backoff(delay = 100))
    public boolean pushDataToLake(DataEnvelope dataEnvelope) throws JsonProcessingException {
        String dataToPush = objectMapper.writeValueAsString(dataEnvelope);
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(HADOOP_URL, dataToPush, String.class);
            if (response.getStatusCode() == HttpStatus.OK) {
                log.info("Status ok");
            }
            return true;
        } catch (HttpServerErrorException httpServerErrorException) {
            if (httpServerErrorException.getRawStatusCode() == HttpStatus.GATEWAY_TIMEOUT.value()) {
                log.info("Hadoop server timeout, retrying");
                throw new RetryException("Hadoop time out, retrying");
            }
            return false;
        }
    }
}
