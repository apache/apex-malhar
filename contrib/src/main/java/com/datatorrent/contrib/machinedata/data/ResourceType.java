package com.datatorrent.contrib.machinedata.data;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 *
 * @since 0.3.5
 */
public enum ResourceType {

    CPU("cpu"), RAM("ram"), HDD("hdd");

    private static Map<String, ResourceType> descToResource = Maps.newHashMap();

    static {
        for (ResourceType type : ResourceType.values()) {
            descToResource.put(type.desc, type);
        }
    }

    private String desc;

    private ResourceType(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return desc;
    }

    public static ResourceType getResourceTypeOf(String desc) {
        return descToResource.get(desc);
    }
}
