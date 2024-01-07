package com.db.dataplatform.techtest.server.api.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.when;

import com.db.dataplatform.techtest.TestDataHelper;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.RetryException;
import org.springframework.web.client.RestTemplate;

@RunWith(MockitoJUnitRunner.class)
public class DataLakePushTest {

    private static final String HADOOP_URL = "http://localhost:8090/hadoopserver/pushbigdata";

    @Spy
    private ObjectMapper objectMapper;

    @Mock
    private RestTemplate restTemplate;

    @InjectMocks
    private DataLakePush dataLakePushMock;

    private DataEnvelope testDataEnvelope;

    @Before
    public void setUp() {
        testDataEnvelope = TestDataHelper.createTestDataEnvelopeApiObject();
    }

    @Test
    public void testShouldSendDataToHadoopAndStatusIsOk() throws JsonProcessingException {
        when(restTemplate.postForEntity(HADOOP_URL, objectMapper.writeValueAsString(testDataEnvelope), String.class))
                .thenReturn(ResponseEntity.ok().build());

        boolean status = dataLakePushMock.pushDataToLake(testDataEnvelope);
        assertThat(status).isTrue();
    }

    @Test
    public void testSendingDataReceivesTimeOut() throws JsonProcessingException {

        when(restTemplate.postForEntity(HADOOP_URL, objectMapper.writeValueAsString(testDataEnvelope), String.class))
                .thenReturn(ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).build());

        try {
            dataLakePushMock.pushDataToLake(testDataEnvelope);
        } catch (RetryException retryException) {
            fail("Hadoop time out, retrying");
        }
    }

}