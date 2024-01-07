package com.db.dataplatform.techtest.server.api.controller;

import javax.validation.Valid;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

@Slf4j
@Controller
@RequestMapping("/dataserver")
@RequiredArgsConstructor
@Validated
public class ServerController {

    private final Server server;
    private final DataLakePush dataLakePush;

    @PostMapping(value = "/pushdata", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Boolean> pushData(@Valid @RequestBody DataEnvelope dataEnvelope) throws IOException, NoSuchAlgorithmException {

        log.info("Data envelope received: {}", dataEnvelope.getDataHeader().getName());
        boolean checksumPass = server.saveDataEnvelope(dataEnvelope);

        log.info("Data envelope persisted. Attribute name: {}", dataEnvelope.getDataHeader().getName());
        dataLakePush.pushDataToLake(dataEnvelope);
        return ResponseEntity.ok(checksumPass);
    }

    @GetMapping(value = "/data/{blockType}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<DataEnvelope>> getData(@PathVariable BlockTypeEnum blockType) {
        log.info("Finding data envelopes for block type {}", blockType);
        List<DataEnvelope> dataBodyEntitiesForType = server.getDataEnvelopeForType(blockType);
        log.info("Found data envelope of size {}", dataBodyEntitiesForType.size());
        return ResponseEntity.ok(dataBodyEntitiesForType);
    }

    @PatchMapping(value = "/update/{name}/{newBlockType}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Boolean> updateData(@PathVariable String name, @PathVariable @Valid BlockTypeEnum newBlockType) {
        log.info("Request received to update name {} with type {}", name, newBlockType);
        return ResponseEntity.ok(server.updateDataEnvelopeBlockNameToType(name, newBlockType));
    }

}
