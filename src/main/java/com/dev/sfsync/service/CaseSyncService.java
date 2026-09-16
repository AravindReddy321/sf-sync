package com.dev.sfsync.service;

import com.dev.sfsync.dao.Case;
import com.dev.sfsync.exception.SfSyncException;
import com.dev.sfsync.repository.CaseRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class CaseSyncService {
    private static final Logger logger = LoggerFactory.getLogger(CaseSyncService.class);
    private final CaseRepository caseRepository;

    public CaseSyncService(CaseRepository caseRepository){
        this.caseRepository = caseRepository;
    }

    public Instant getRecentCaseLastModifiedDate(){

        try {
            logger.info("inside getRecentCaseClosedDate");
            return caseRepository.
                    findTopByOrderByLastModifiedDateDesc().map(Case::getCreatedDate).orElse(Instant.EPOCH);
//                        findTopByOrderByClosedDateDesc().map(Case::getClosedDate).orElse(Instant.EPOCH);
        } catch (Exception e) {
            logger.error("error in getRecentCaseClosedDate {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }
}
