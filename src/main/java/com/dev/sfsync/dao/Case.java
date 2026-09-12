package com.dev.sfsync.dao;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;

import java.time.Instant;

@Entity
@Table(name ="Cases")
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Case {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name="sf_id", unique = true)
    private String sfId;

    @Column(name="subject")
    private String subject;

    @Column(name="priority")
    private String priority;

    @Column(name="status")
    private String status;

    @Column(name="reason")
    private String reason;

    @Column(name="closed_date")
    private Instant closedDate;

    @CreatedDate
    @Column(name="created_date",updatable=false)
    private Instant createdDate;

    @LastModifiedDate
    @Column(name="last_modified_date")
    private Instant lastModifiedDate;
}
