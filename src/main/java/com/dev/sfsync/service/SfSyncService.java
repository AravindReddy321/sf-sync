package com.dev.sfsync.service;

import com.dev.sfsync.dao.Account;
import com.dev.sfsync.dao.AccountMapper;
import com.dev.sfsync.dto.AccountDto;
import com.dev.sfsync.repository.AccountRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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

    public SfSyncService(AccountRepository accountRepository,AccountMapper accountMapper){
        this.accountRepository =accountRepository;
        this.accountMapper = accountMapper;
    }

    public void syncAccount(AccountDto accountDto) throws RuntimeException{
        Account acc = this.accountMapper.convertAccountDtoToAccount(accountDto);
//        if(this.accountRepository.existsBySfId(accountDto.Id())){
            acc.id = this.accountRepository.findBySfId(accountDto.id()).map(Account::getId).orElse(null);
//        }
        this.accountRepository.save(acc);
    }

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

    public Instant getLastSyncTime(){
        return accountRepository.findTopByOrderByLastSyncTimeDesc().map(Account::getLastSyncTime).orElse(Instant.EPOCH);
    }

    public List<Account> fetchAccountsBySfIds(Set<String> sfIds){
        logger.info("fetchAccountsBySfIds {}", List.copyOf(sfIds));
        return accountRepository.findAllBySfIdIsIn(List.copyOf(sfIds));
//        boolean accountExsits = accountRepository.existsBySfId("001Hs00002xhRyPIAU");
//        logger.info("exsits Account by sf id 001Hs00002xhRyPIAU {}", accountExsits);
//        for(String sfId : sfIds){
//        try{
//            accountRepository.findBySfId("001Hs00002xhRyPIAU");
//        } catch(Exception e){
//            logger.error("fetchAccountsBySfIds error", e);
//        }
//            logger.info("find by sf id ");
//            boolean accountExsits = accountRepository.existsBySfId("001Hs00002xhRyPIAU");
//            logger.info("exsits Account by sf id 001Hs00002xhRyPIAU {}", accountExsits);
////        }
//        return accountRepository.findAll();
    }
}
