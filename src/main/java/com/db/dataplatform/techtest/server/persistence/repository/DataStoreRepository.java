package com.db.dataplatform.techtest.server.persistence.repository;

import java.util.List;
import java.util.Optional;

import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DataStoreRepository extends JpaRepository<DataBodyEntity, Long> {

    List<DataBodyEntity> findDataBodyEntitiesByDataHeaderEntityBlocktype(BlockTypeEnum blockType);

    Optional<DataBodyEntity> findDataBodyEntityByDataHeaderEntityName(String blockName);
}
