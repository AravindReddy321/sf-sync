package com.dev.sfsync.controller;

import com.dev.sfsync.dto.AccountDto;
import com.dev.sfsync.dto.SfQueryResponseWrapper;
import com.dev.sfsync.exception.SfSyncException;
import com.dev.sfsync.service.SfSyncService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriBuilder;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
public class SfSyncController {


    private final RestClient restClient;
    private final SfSyncService sfSyncService;

    private final String salesforceEndpoint;
    private final String clientId;
    private final String clientSecret;

    private String accessToken;
    private Instant expirationTime;

    public SfSyncController(RestClient restClient,
                            SfSyncService sfSyncService,
                            @Value("${sf.uri}") String salesforceEndpoint,
                            @Value("${sf.clientid}") String clientId,
                            @Value("${sf.clientsecret}") String clientSecret){
        this.restClient = restClient;
        this.sfSyncService =sfSyncService;
        this.salesforceEndpoint = salesforceEndpoint;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    @GetMapping(path = "/public")
    public String getHello(){
        return "hello world";
    }

    public void setAccessToken(){
        if(this.accessToken == null || Instant.now().plusSeconds(60).isAfter(expirationTime)){
            this.accessToken = this.fetchAccessToken();
        }
    }

    public String fetchAccessToken(){
        MultiValueMap<String, String> formData = new LinkedMultiValueMap<>();
        formData.add("grant_type","client_credentials");
        formData.add("client_id",this.clientId);
        formData.add("client_secret",this.clientSecret);
        String accessToken = (String)restClient.post()
                .uri(this.salesforceEndpoint+"/services/oauth2/token")
                .body(formData)
                .retrieve()
                .body(Map.class).get("access_token");
        if(accessToken != null){
            this.expirationTime = Instant.now().plusSeconds(7200);
        }
        return accessToken;
    }

    @GetMapping(path="/account/{id}")
    public String syncAccount(@PathVariable String id){
        setAccessToken();
        AccountDto accountDto = restClient.get()
                .uri(this.salesforceEndpoint+"/services/data/v60.0/sobjects/Account/{id}",id)
                .header("Authorization","Bearer "+this.accessToken)
                .retrieve()
                .body(AccountDto.class);
//        return "Success";
        try{
            sfSyncService.syncAccount(accountDto);
        } catch (RuntimeException e) {
            throw new SfSyncException("error in sync");
        }
        return this.accessToken + " "+accountDto.toString();
    }

    @GetMapping(path = "/syncAccounts")
    public ResponseEntity<String> syncAccounts(){
        setAccessToken();

        SfQueryResponseWrapper<AccountDto> sfQueryResponseWrapper = restClient.get()
                .uri(this.salesforceEndpoint+"/services/data/v60.0/query?q=select+id+from+account")
                .header("Authorization","Bearer "+accessToken)
                .retrieve()
                .body(new ParameterizedTypeReference<SfQueryResponseWrapper<AccountDto>>(){});
//        if (sfQueryResponseWrapper != null) {
//            this.sfSyncService.syncAccounts(sfQueryResponseWrapper.records());
//        }
        return new ResponseEntity<String>("Sucess: "+sfQueryResponseWrapper.records(), HttpStatus.OK);
    }

    @GetMapping(path = "/syncAccountsDelta")
    public ResponseEntity<String> syncAccountsDelta(){
        setAccessToken();
        Instant lastSyncTime = sfSyncService.getLastSyncTime();
        String soql = "SELECT Id,Name FROM Account WHERE lastModifiedDate >= "+lastSyncTime;
        URI deltaUri = UriComponentsBuilder
                .fromUriString(this.salesforceEndpoint+"/services/data/v60.0/query")
                        .queryParam("q",soql)
                                .build().toUri();
        SfQueryResponseWrapper<AccountDto> sfQueryResponseWrapper = restClient.get()
                .uri(deltaUri)
                .header("Authorization","Bearer "+accessToken)
                .retrieve()
                .body(new ParameterizedTypeReference<SfQueryResponseWrapper<AccountDto>>() {
                });
        sfSyncService.syncAccounts(sfQueryResponseWrapper.records());
        String response = String.format("Success. lastSyncTime: %s. No of records synced: %d",lastSyncTime, sfQueryResponseWrapper.records().size());
        return new ResponseEntity<String>(response , HttpStatus.OK);
    }


    // This means: "Starting at minute 0, run every 2 minutes"
    // @Scheduled(cron = "0 */2 * * * ?")
    @Scheduled(fixedDelay = 30000)
    public void syncAccountsDeltaScheduled(){
        syncAccountsDelta();
    }

}
