package com.dev.sfsync.service;

import com.dev.sfsync.dao.Account;
import com.dev.sfsync.dao.AccountMapper;
import com.dev.sfsync.dto.AccountDto;
import com.dev.sfsync.repository.AccountRepository;
import jakarta.transaction.Transactional;
import org.hibernate.exception.JDBCConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.CannotCreateTransactionException;

import java.net.ConnectException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class SfSyncService {

    private final Logger logger = LoggerFactory.getLogger(SfSyncService.class);

    private final AccountRepository accountRepository;
    private final AccountMapper accountMapper;
    private final DlqService dlqService;

    public SfSyncService(AccountRepository accountRepository,AccountMapper accountMapper, DlqService dlqService){
        this.accountRepository =accountRepository;
        this.accountMapper = accountMapper;
        this.dlqService = dlqService;
    }

    @Transactional
    @Retryable(
            retryFor = {JDBCConnectionException.class,
                    DataAccessResourceFailureException.class,
                    CannotCreateTransactionException.class
            },
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000)
    )
    public boolean syncAccount(AccountDto accountDto){
        try{
            Account acc = this.accountMapper.convertAccountDtoToAccount(accountDto);
            acc.id = this.accountRepository.findBySfId(accountDto.id()).map(Account::getId).orElse(null);
            this.accountRepository.save(acc);
            return true;
        } catch (DataIntegrityViolationException e){
            return false;
        }
    }

    @Transactional
    @Retryable(
            retryFor = {JDBCConnectionException.class,
                    DataAccessResourceFailureException.class,
                    CannotCreateTransactionException.class
            },
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000)
    )
    public void syncAccounts(List<AccountDto> accountDtoList){
        List<Account> accountList = accountDtoList.stream()
                .map(accountDto -> {
                    Account acc = this.accountMapper.convertAccountDtoToAccount(accountDto);
                    acc.id = this.accountRepository.findBySfId(accountDto.id()).map(Account::getId).orElse(null);
                    return acc;
                })
                .toList();
        this.accountRepository.saveAll(accountList);
    }

    @Recover
    public boolean syncAccountRecover(Exception e, AccountDto accountDto){
        logger.error("error in syncAccountRecover e {}",e.toString());
        if(accountDto != null){
            dlqService.moveToDlq(accountDto);
        }
        return false;
    }

    @Recover
    public void syncAccountsRecover(Exception e, List<AccountDto> accountDtoList){
        logger.error("error in SyncAccountsRecover e {}", e.toString());
        if(!accountDtoList.isEmpty()){
            dlqService.moveToDlq(accountDtoList);
        }
    }

    public Instant getLastSyncTime(){
        return accountRepository.findTopByOrderByLastSyncTimeDesc().map(Account::getLastSyncTime).orElse(Instant.EPOCH);
    }

    @Retryable(
            retryFor = {
                    JDBCConnectionException.class,
                    CannotCreateTransactionException.class
            },
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000)
    )
    public List<Account> fetchAccountsBySfIds(Set<String> sfIds){
        logger.info("fetchAccountsBySfIds {}", List.copyOf(sfIds));
        return accountRepository.findAllBySfIdIsIn(List.copyOf(sfIds));
    }

    @Recover
    public List<Account> fetchAccountsBySfIdsRecover(Exception e, Set<String> sfIds) throws Exception {
        logger.error("error in fetchAccountsBySfIdsRecover e {}",e.toString());
//        dlqService.moveToDlq(sfIds);
//        return  new ArrayList<>();
        throw e;
    }
}
