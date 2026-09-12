package com.dev.sfsync.repository;

import com.dev.sfsync.dao.Case;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface CaseRepository extends JpaRepository<Case, Long> {
    Optional<Case> findTopByOrderByClosedDateDesc();

    Optional<Case> findTopByOrderByLastModifiedDateDesc();
}
