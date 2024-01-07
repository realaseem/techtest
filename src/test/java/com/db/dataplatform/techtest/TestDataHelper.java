package com.db.dataplatform.techtest;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.MD5;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;

public class TestDataHelper {

    private static final int SIGNUM = 1;
    private static final int RADIX = 16;
    public static final String TEST_NAME = "Test";
    public static final String TEST_NAME_EMPTY = "";
    public static final String DUMMY_DATA = "AKCp5fU4WNWKBVvhXsbNhqk33tawri9iJUkA5o4A6YqpwvAoYjajVw8xdEw6r9796h1wEp29D";
    public static final String CHECKSUM = calculateHash();

    public static DataHeaderEntity createTestDataHeaderEntity(Instant expectedTimestamp) {
        DataHeaderEntity dataHeaderEntity = new DataHeaderEntity();
        dataHeaderEntity.setName(TEST_NAME);
        dataHeaderEntity.setBlocktype(BlockTypeEnum.BLOCKTYPEA);
        dataHeaderEntity.setCreatedTimestamp(expectedTimestamp);
        return dataHeaderEntity;
    }

    public static DataBodyEntity createTestDataBodyEntity(DataHeaderEntity dataHeaderEntity) {
        DataBodyEntity dataBodyEntity = new DataBodyEntity();
        dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);
        dataBodyEntity.setDataBody(DUMMY_DATA);
        return dataBodyEntity;
    }

    public static DataEnvelope createTestDataEnvelopeApiObject() {
        DataBody dataBody = new DataBody(DUMMY_DATA);
        DataHeader dataHeader = new DataHeader(TEST_NAME, BlockTypeEnum.BLOCKTYPEA, CHECKSUM);

        return new DataEnvelope(dataHeader, dataBody);
    }

    public static DataEnvelope createTestDataEnvelopeApiObjectWithEmptyName() {
        DataBody dataBody = new DataBody(DUMMY_DATA);
        DataHeader dataHeader = new DataHeader(TEST_NAME_EMPTY, BlockTypeEnum.BLOCKTYPEA, CHECKSUM);

        return new DataEnvelope(dataHeader, dataBody);
    }

    public static DataEnvelope createTestDataEnvelopeApiObjectWithChecksum(String checksum) {
        DataBody dataBody = new DataBody(DUMMY_DATA);
        DataHeader dataHeader = new DataHeader(TEST_NAME, BlockTypeEnum.BLOCKTYPEA, checksum);

        return new DataEnvelope(dataHeader, dataBody);
    }

    private static String calculateHash() {
        try {
            byte[] hash = MessageDigest.getInstance(MD5).digest(DUMMY_DATA.getBytes(UTF_8));
            return new BigInteger(SIGNUM, hash).toString(RADIX);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
