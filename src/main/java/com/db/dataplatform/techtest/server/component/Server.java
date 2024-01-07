package com.db.dataplatform.techtest.server.component;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;

public interface Server {
    boolean saveDataEnvelope(DataEnvelope envelope) throws IOException, NoSuchAlgorithmException;

    List<DataEnvelope> getDataEnvelopeForType(BlockTypeEnum blockTypeEnum);

    boolean updateDataEnvelopeBlockNameToType(String blockName, BlockTypeEnum blockTypeEnum);
}
