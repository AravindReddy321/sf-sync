package com.dev.sfsync.dao;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.time.LocalDateTime;

@Entity
@Table(name = "Accounts")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class Account {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    public Long id;

    @Column(name = "sf_id", unique = true)
    public String sfId;

    @Column(name ="name")
    public String name;

    @Column(name = "description")
    public String description;

    @Column(name ="is_deleted")
    public Boolean isDeleted;

    @Column(name="last_sync_time")
    public Instant lastSyncTime;
}
