package com.cihangurkan.cache.entities;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class CacheEntity implements Serializable {

    private Serializable object;
    private Date date;
    private UUID uuidLock;
}
