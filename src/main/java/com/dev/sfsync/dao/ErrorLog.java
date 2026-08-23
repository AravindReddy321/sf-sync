package com.dev.sfsync.dao;

import jakarta.persistence.*;
import lombok.*;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;

@Entity
@Table(name="error_logs")
@Builder
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ErrorLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name="id")
    private Long Id;

    @Column(name="message")
    private String message;

    @Column(name="process_name")
    private String processName;

    @Column(name="record_id")
    private String recordId;

    @Column(name="source_name")
    private String sourceName;

    @Column(name="exception_type")
    private String exceptionType;

    @Lob
    @Column(name="stack_trace", columnDefinition = "LONGTEXT")
    private String stackTrace;

    @LastModifiedDate
    @Column(name="last_modified_date", updatable=false)
    private String lastModfiedDate;

    @CreatedDate
    @Column(name="created_date", updatable=false)
    private String createdDate;
}
