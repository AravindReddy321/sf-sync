package com.dev.sfsync.dao;

import com.dev.sfsync.dto.AccountDto;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class AccountMapper {

    public Account convertAccountDtoToAccount(AccountDto accountDto){
        return Account.builder()
                .name(accountDto.Name())
                .sfId(accountDto.Id())
                .lastSyncTime(Instant.now())
                .build();
    }
}
