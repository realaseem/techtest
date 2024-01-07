package com.db.dataplatform.techtest.api.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import java.util.ArrayList;
import java.util.List;

import com.db.dataplatform.techtest.TestDataHelper;
import com.db.dataplatform.techtest.server.api.controller.DataLakePush;
import com.db.dataplatform.techtest.server.api.controller.ServerController;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.util.UriTemplate;

@RunWith(MockitoJUnitRunner.class)
public class ServerControllerComponentTest {

    public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
    public static final UriTemplate URI_GETDATA = new UriTemplate("http://localhost:8090/dataserver/data/{blockType}");
    public static final UriTemplate URI_PATCHDATA = new UriTemplate("http://localhost:8090/dataserver/update/{name}/{newBlockType}");

    @Mock
    private Server serverMock;

    private DataEnvelope testDataEnvelope;
    private ObjectMapper objectMapper;
    private MockMvc mockMvc;
    @Mock
    private DataLakePush dataLakePushMock;

    @Before
    public void setUp() {
        ServerController serverController = new ServerController(serverMock, dataLakePushMock);
        mockMvc = standaloneSetup(serverController).build();
        objectMapper = Jackson2ObjectMapperBuilder
                .json()
                .build();

        testDataEnvelope = TestDataHelper.createTestDataEnvelopeApiObject();
    }

    @Test
    public void testPushDataPostCallWorksAsExpected() throws Exception {
        when(serverMock.saveDataEnvelope(any(DataEnvelope.class))).thenReturn(true);
        when(dataLakePushMock.pushDataToLake(any(DataEnvelope.class))).thenReturn(true);

        String testDataEnvelopeJson = objectMapper.writeValueAsString(testDataEnvelope);

        MvcResult mvcResult = mockMvc.perform(post(URI_PUSHDATA)
                        .content(testDataEnvelopeJson)
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk())
                .andReturn();

        boolean checksumPass = Boolean.parseBoolean(mvcResult.getResponse().getContentAsString());
        assertThat(checksumPass).isTrue();
        verify(dataLakePushMock).pushDataToLake(any(DataEnvelope.class));
    }

    @Test
    public void testGetDataGetCallWorksAsExpected() throws Exception {
        List<DataEnvelope> envelopeList = new ArrayList<>();
        envelopeList.add(testDataEnvelope);
        when(serverMock.getDataEnvelopeForType(any(BlockTypeEnum.class))).thenReturn(envelopeList);

        MvcResult mvcResult = mockMvc.perform(get(URI_GETDATA.toString(), BlockTypeEnum.BLOCKTYPEA.name())
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk())
                .andReturn();
        List<DataEnvelope> envelopes = objectMapper.readValue(mvcResult.getResponse().getContentAsString(),
                new TypeReference<List<DataEnvelope>>() {});
        assertThat(testDataEnvelope.getDataBody()).isEqualToComparingFieldByField(envelopes.get(0).getDataBody());
        assertThat(testDataEnvelope.getDataHeader()).isEqualToComparingFieldByField(envelopes.get(0).getDataHeader());
    }

    @Test
    public void testPatchDataCallWorksAsExpected() throws Exception {
        String name = "testName";
        BlockTypeEnum type = BlockTypeEnum.BLOCKTYPEA;
        when(serverMock.updateDataEnvelopeBlockNameToType(name, type)).thenReturn(true);

        MvcResult mvcResult = mockMvc.perform(patch(URI_PATCHDATA.toString(), name, type)
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk())
                .andReturn();
        boolean checksumPass = Boolean.parseBoolean(mvcResult.getResponse().getContentAsString());
        assertThat(checksumPass).isTrue();
    }

}
