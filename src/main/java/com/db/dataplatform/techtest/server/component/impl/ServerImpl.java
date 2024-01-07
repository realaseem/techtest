package com.db.dataplatform.techtest.server.component.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.MD5;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class ServerImpl implements Server {

    private static final int SIGNUM = 1;
    private static final int RADIX = 16;

    private final DataBodyService dataBodyServiceImpl;
    private final ModelMapper modelMapper;

    /**
     * @param envelope the envelope
     * @return true if there is a match with the client provided checksum.
     */
    @Override
    @Transactional
    public boolean saveDataEnvelope(DataEnvelope envelope) throws NoSuchAlgorithmException {

        // Save to persistence.
        persist(envelope);

        log.info("Data persisted successfully, data name: {}", envelope.getDataHeader().getName());
        String calculatedHash = calculateHash(envelope.getDataBody().getDataBody());
        return calculatedHash.equals(envelope.getDataHeader().getChecksum());
    }

    private void persist(DataEnvelope envelope) {
        log.info("Persisting data with attribute name: {}", envelope.getDataHeader().getName());
        DataHeaderEntity dataHeaderEntity = modelMapper.map(envelope.getDataHeader(), DataHeaderEntity.class);

        DataBodyEntity dataBodyEntity = modelMapper.map(envelope.getDataBody(), DataBodyEntity.class);
        dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);

        saveData(dataBodyEntity);
    }

    private void saveData(DataBodyEntity dataBodyEntity) {
        dataBodyServiceImpl.saveDataBody(dataBodyEntity);
    }

    private String calculateHash(String data) throws NoSuchAlgorithmException {
        byte[] hash = MessageDigest.getInstance(MD5).digest(data.getBytes(UTF_8));
        return new BigInteger(SIGNUM, hash).toString(RADIX);
    }

    @Override
    public List<DataEnvelope> getDataEnvelopeForType(BlockTypeEnum blockTypeEnum) {
        List<DataBodyEntity> entities = dataBodyServiceImpl.getDataByBlockType(blockTypeEnum);

        return entities.stream().map(entity ->
                        new DataEnvelope(convertHeader(entity.getDataHeaderEntity()), convertBody(entity)))
                .collect(Collectors.toList());
    }

    private DataHeader convertHeader(DataHeaderEntity dataHeader) {
        return new DataHeader(dataHeader.getName(), dataHeader.getBlocktype(), dataHeader.getChecksum());
    }

    private DataBody convertBody(DataBodyEntity dataBodyEntity) {
        return new DataBody(dataBodyEntity.getDataBody());
    }

    @Override
    @Transactional
    public boolean updateDataEnvelopeBlockNameToType(String blockName, BlockTypeEnum blockTypeEnum) {
        Optional<DataBodyEntity> dataBodyEntity = dataBodyServiceImpl.getDataByBlockName(blockName);
        if(dataBodyEntity.isPresent()) {
            log.info("Found data with blockName: {}, updating type", blockName);
            DataBodyEntity entity = dataBodyEntity.get();
            DataHeaderEntity dataHeaderEntity = entity.getDataHeaderEntity();
            dataHeaderEntity.setBlocktype(blockTypeEnum);
            entity.setDataHeaderEntity(dataHeaderEntity);
            saveData(entity);
            return true;
        }
        return false;
    }
}
