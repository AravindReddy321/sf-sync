package com.dev.sfsync.client;

import com.dev.sfsync.dto.AccountDto;
import com.dev.sfsync.exception.SfSyncException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClient;

import java.time.Instant;
import java.util.Map;

@Component
@Getter
public class SfSyncClient {

    private static final Logger logger = LoggerFactory.getLogger(SfSyncClient.class);

    private final RestClient restClient;
    private final String salesforceBaseEndpoint;
    private final String clientId;
    private final String clientSecret;

    private static final String CLIENT_CRED_AUTH_FLOW = "client_credentials";
    private static final String BEARER = "Bearer";
    private static final String AUTHORIZATION = "Authorization";

    private String accessToken;
    private Instant expirationTime;

    public SfSyncClient( RestClient restClient, String sfUriBean, String clientIdBean, String clientSecret

    ){
        this.restClient = restClient;
        this.salesforceBaseEndpoint = sfUriBean;
        this.clientId = clientIdBean;
        this.clientSecret = clientSecret;
    }

    public boolean isAccessTokenExpired(){
        try {
            return (this.accessToken ==  null || Instant.now().plusSeconds(60).isAfter(expirationTime));
        } catch (Exception e) {
            logger.error("error in is accessToken expired {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    public String getTokenServiceUrl(){
        return salesforceBaseEndpoint+"/services/oauth2/token";
    }

    public String getAccessToken(){
        try{
            if(isAccessTokenExpired()) setAccessToken();
            return this.accessToken;
        } catch(Exception e){
            logger.error("error in getAccessToken {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    public void setAccessToken(){
        try{
            MultiValueMap<String, String> formData = new LinkedMultiValueMap<>();
            formData.add("grant_type",CLIENT_CRED_AUTH_FLOW);
            formData.add("client_id",clientId);
            formData.add("client_secret",clientSecret);

            Map<?,?> resposneMap = restClient.post()
                    .uri(getTokenServiceUrl())
                    .body(formData)
                    .retrieve()
                    .body(Map.class);
            if(resposneMap != null && resposneMap.containsKey("access_token")){
                String token = (String) resposneMap.get("access_token");
                if(token != null && !token.isBlank()){
                    accessToken = token;
                    expirationTime = Instant.now().plusSeconds(7200);
                    return;
                }
            }

            throw new SfSyncException("Failed to fetch accesstoken, response body was null or missing accesstoken");
        } catch(Exception e){
            logger.error("error in setAccessToken {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }


    public Map<String, Object> submitPostApiRequest(String uri, Map<String, String> bodyMap){
        try {
            return restClient.post()
                    .uri(uri)
                    .header(AUTHORIZATION, BEARER+" "+accessToken)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(bodyMap)
                    .retrieve()
                    .body(new ParameterizedTypeReference<Map<String, Object>>(){});
        } catch (Exception e) {
            logger.error("error in submitPostApiRequest {}", e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    public Map<String, Object> submitGetApiRequest(String uri){
        try {
            return restClient.get()
                    .uri(uri)
                    .header(AUTHORIZATION, BEARER+" "+accessToken)
                    .retrieve()
                    .body(new ParameterizedTypeReference<Map<String, Object>>(){});
        } catch (Exception e) {
            logger.error("error in submitGetApiRequest {}", e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    public Resource submitGetApiRequestCsv(String uri){
        return restClient.get()
                .uri(uri)
                .header(AUTHORIZATION, BEARER+" "+accessToken)
                .retrieve()
                .body(Resource.class);
    }

    public AccountDto syncSingleAccount(String accountId){
        try {
            return restClient.get()
                    .uri(this.salesforceBaseEndpoint+"/services/data/v60.0/sobjects/Account/{accountId}",accountId)
                    .header(AUTHORIZATION,BEARER+" "+getAccessToken())
                    .retrieve()
                    .body(AccountDto.class);
        } catch (Exception e) {
            logger.error("error in syncSingleAccount {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }


}
