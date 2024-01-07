package com.db.dataplatform.techtest.service;

import static com.db.dataplatform.techtest.TestDataHelper.createTestDataEnvelopeApiObject;
import static com.db.dataplatform.techtest.TestDataHelper.createTestDataEnvelopeApiObjectWithChecksum;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.component.impl.ServerImpl;
import com.db.dataplatform.techtest.server.mapper.ServerMapperConfiguration;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.modelmapper.ModelMapper;

@RunWith(MockitoJUnitRunner.class)
public class ServerServiceTests {

    @Mock
    private DataBodyService dataBodyServiceImplMock;

    private DataBodyEntity expectedDataBodyEntity;
    private DataEnvelope testDataEnvelope;

    @Captor
    private ArgumentCaptor<DataBodyEntity> dataBodyEntityArgumentCaptor;

    private Server server;

    @Before
    public void setup() {
        ServerMapperConfiguration serverMapperConfiguration = new ServerMapperConfiguration();
        ModelMapper modelMapper = serverMapperConfiguration.createModelMapperBean();

        testDataEnvelope = createTestDataEnvelopeApiObject();
        expectedDataBodyEntity = modelMapper.map(testDataEnvelope.getDataBody(), DataBodyEntity.class);
        expectedDataBodyEntity.setDataHeaderEntity(modelMapper.map(testDataEnvelope.getDataHeader(),
                DataHeaderEntity.class));
        server = new ServerImpl(dataBodyServiceImplMock, modelMapper);
    }

    @Test
    public void shouldSaveDataEnvelopeAsExpected() throws NoSuchAlgorithmException, IOException {

        boolean success = server.saveDataEnvelope(testDataEnvelope);

        assertThat(success).isTrue();

        verify(dataBodyServiceImplMock).saveDataBody(dataBodyEntityArgumentCaptor.capture());

        DataBodyEntity dataBodyCapturedObject = dataBodyEntityArgumentCaptor.getValue();
        assertThat(dataBodyCapturedObject.getDataHeaderEntity())
                .isEqualToComparingFieldByField(expectedDataBodyEntity.getDataHeaderEntity());
        assertThat(dataBodyCapturedObject.getDataBody()).isEqualTo(expectedDataBodyEntity.getDataBody());
        assertThat(dataBodyCapturedObject.getDataStoreId()).isEqualTo(expectedDataBodyEntity.getDataStoreId());
        assertThat(dataBodyCapturedObject.getCreatedTimestamp()).isEqualTo(expectedDataBodyEntity.getCreatedTimestamp());
    }

    @Test
    public void shouldSaveDataEnvelopeReturnFalseWhenChecksumDoesNotMatch() throws IOException, NoSuchAlgorithmException {
        String dummyChecksum = "dummyChecksum";
        testDataEnvelope = createTestDataEnvelopeApiObjectWithChecksum(dummyChecksum);

        boolean success = server.saveDataEnvelope(testDataEnvelope);

        assertThat(success).isFalse();
    }

    @Test
    public void shouldGetDataEnvelopeAsExpected() {
        List<DataBodyEntity> dataBodyEntityList = new ArrayList<>();
        dataBodyEntityList.add(expectedDataBodyEntity);

        when(dataBodyServiceImplMock.getDataByBlockType(BlockTypeEnum.BLOCKTYPEA)).thenReturn(dataBodyEntityList);

        List<DataEnvelope> dataEnvelopes = server.getDataEnvelopeForType(BlockTypeEnum.BLOCKTYPEA);

        assertThat(dataEnvelopes).hasSize(1);
        DataHeader dataHeader = dataEnvelopes.get(0).getDataHeader();
        DataBody dataBody = dataEnvelopes.get(0).getDataBody();

        DataHeaderEntity dataHeaderEntity = expectedDataBodyEntity.getDataHeaderEntity();
        assertThat(dataHeader.getBlockType()).isEqualTo(dataHeaderEntity.getBlocktype());
        assertThat(dataHeader.getName()).isEqualTo(dataHeaderEntity.getName());
        assertThat(dataHeader.getChecksum()).isEqualTo(dataHeaderEntity.getChecksum());
        assertThat(dataBody.getDataBody()).isEqualTo(expectedDataBodyEntity.getDataBody());
    }

    @Test
    public void shouldUpdateDataEnvelopeAsExpectedAndReturnTrue() {
        String name = "blockName";
        BlockTypeEnum newType = BlockTypeEnum.BLOCKTYPEB;
        when(dataBodyServiceImplMock.getDataByBlockName(name)).thenReturn(Optional.of(expectedDataBodyEntity));

        boolean updated = server.updateDataEnvelopeBlockNameToType(name, newType);

        assertThat(updated).isTrue();

        verify(dataBodyServiceImplMock).saveDataBody(dataBodyEntityArgumentCaptor.capture());

        DataBodyEntity dataBodyCapturedObject = dataBodyEntityArgumentCaptor.getValue();
        assertThat(dataBodyCapturedObject.getDataHeaderEntity().getBlocktype()).isEqualTo(newType);
    }

    @Test
    public void shouldUpdateDataEnvelopeAsExpectedAndReturnFalse() {
        String name = "blockName";
        BlockTypeEnum newType = BlockTypeEnum.BLOCKTYPEB;
        when(dataBodyServiceImplMock.getDataByBlockName(name)).thenReturn(Optional.empty());

        boolean updated = server.updateDataEnvelopeBlockNameToType(name, newType);

        assertThat(updated).isFalse();

        verify(dataBodyServiceImplMock, times(0)).saveDataBody(any(DataBodyEntity.class));
    }

}
