package com.dev.sfsync.service;

import com.dev.sfsync.dao.Account;
import com.dev.sfsync.dao.AccountMapper;
import com.dev.sfsync.dto.AccountDto;
import com.dev.sfsync.repository.AccountRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class SfSyncService {

    private final AccountRepository accountRepository;
    private final AccountMapper accountMapper;

    public SfSyncService(AccountRepository accountRepository,AccountMapper accountMapper){
        this.accountRepository =accountRepository;
        this.accountMapper = accountMapper;
    }

    public void syncAccount(AccountDto accountDto) throws RuntimeException{
        Account acc = this.accountMapper.convertAccountDtoToAccount(accountDto);
//        if(this.accountRepository.existsBySfId(accountDto.Id())){
            acc.Id = this.accountRepository.findBySfId(accountDto.Id()).map(Account::getId).orElse(null);
//        }
        this.accountRepository.save(acc);
    }

    public void syncAccounts(List<AccountDto> accountDtoList){
        List<Account> accountList = accountDtoList.stream()
                .map(accountDto -> {
                    Account acc = this.accountMapper.convertAccountDtoToAccount(accountDto);
                    acc.Id = this.accountRepository.findBySfId(accountDto.Id()).map(Account::getId).orElse(null);
                    return acc;
                })
                .toList();
        this.accountRepository.saveAll(accountList);
    }

    public Instant getLastSyncTime(){
        return accountRepository.findTopByOrderByLastSyncTimeDesc().map(Account::getLastSyncTime).orElse(Instant.EPOCH);
    }
}
