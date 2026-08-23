package com.dev.sfsync.config;

import com.dev.sfsync.client.SfSyncClient;
import com.salesforce.eventbus.protobuf.PubSubGrpc;
import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.grpc.client.autoconfigure.GrpcClientProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.client.NettyGrpcChannelFactory;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;

import java.io.IOException;
import java.util.Map;

@Configuration
public class SfGrpcConfig {
    public static final Logger logger = LoggerFactory.getLogger(SfGrpcConfig.class);


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
        logger.info("inside blockingStubAuthenticated");
        return blockingStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    @Bean
    public PubSubGrpc.PubSubStub asyncStubAuthenticated(PubSubGrpc.PubSubStub asyncStub, Metadata headers) {
        return asyncStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    @Bean
    public String printGrpcBeans(NettyGrpcChannelFactory nettyGrpcChannelFactory, GrpcClientProperties grpcClientProperties) {
        logger.info("inside printGrpcBeans");
        logger.info("netty grpc channel factory {}", nettyGrpcChannelFactory);
        logger.info("grpc client properties {}", grpcClientProperties.getChannel());
        return "test";
    }


}
