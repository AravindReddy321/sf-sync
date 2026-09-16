package com.dev.sfsync.utility;

import com.google.protobuf.ByteString;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
@Setter
public class RetryCdcState {
    private int retryCount;
    private int numRequested;
    private ByteString replayId;
}
