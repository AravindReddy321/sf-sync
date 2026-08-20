package com.dev.sfsync.config;

import com.dev.sfsync.client.SfSyncClient;
import com.salesforce.eventbus.protobuf.PubSubGrpc;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SfGrpcConfig {


    @Bean
    public Metadata headers(String accessTokenBean, String sfUriBean, String sfOrgIdBean) {

        Metadata headers = new Metadata();

        Metadata.Key<String> authKey = Metadata.Key.of("accesstoken",Metadata.ASCII_STRING_MARSHALLER);
        Metadata.Key<String> instanceUrl = Metadata.Key.of("instanceurl",Metadata.ASCII_STRING_MARSHALLER);
        Metadata.Key<String> tenantid = Metadata.Key.of("tenantid",Metadata.ASCII_STRING_MARSHALLER);

        String accessToken="Bearer "+accessTokenBean;

        headers.put(authKey,accessToken);
        headers.put(instanceUrl,sfUriBean);
        headers.put(tenantid,sfOrgIdBean);

        return headers;
    }

    @Bean
    public PubSubGrpc.PubSubBlockingStub blockingStubAuthenticated(PubSubGrpc.PubSubBlockingStub blockingStub, Metadata headers) {
        return blockingStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    @Bean
    public PubSubGrpc.PubSubStub asyncStubAuthenticated(PubSubGrpc.PubSubStub asyncStub, Metadata headers) {
        return asyncStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }
}
