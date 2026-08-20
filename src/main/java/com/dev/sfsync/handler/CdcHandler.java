package com.dev.sfsync.handler;

import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;

public interface CdcHandler {
    void processEventsData(List<Map<String,Object>> finalDataMapList);

}
