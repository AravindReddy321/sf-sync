package com.dev.sfsync.repository;

import com.dev.sfsync.dao.Account;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Repository
public interface AccountRepository extends JpaRepository<Account, Long> {
    boolean existsBySfId(String sfId);

    Optional<Account> findBySfId(String sfId);

    Optional<Account> findTopByOrderByLastSyncTimeDesc();

    List<Account> findAllBySfIdIsIn(List<String> sfIds);
}
