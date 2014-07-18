package com.presto.odata;

import org.odata4j.edm.EdmSimpleType;
import org.odata4j.edm.EdmType;
import org.odata4j.producer.jdbc.JdbcModelToMetadata;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class PrestoModelToMetadataMapper extends JdbcModelToMetadata {

    private final Map<Integer, EdmType> SIMPLE_TYPE_MAPPING = new HashMap<>();

    {
        SIMPLE_TYPE_MAPPING.put(Types.BIGINT, EdmSimpleType.INT64);
        SIMPLE_TYPE_MAPPING.put(Types.LONGNVARCHAR, EdmSimpleType.STRING);
        SIMPLE_TYPE_MAPPING.put(Types.INTEGER, EdmSimpleType.INT32);
        SIMPLE_TYPE_MAPPING.put(Types.VARCHAR, EdmSimpleType.STRING);
        SIMPLE_TYPE_MAPPING.put(Types.BOOLEAN, EdmSimpleType.BOOLEAN);
        SIMPLE_TYPE_MAPPING.put(-16, EdmSimpleType.STRING);
    }

    @Override
    public EdmType getEdmType(int jdbcType, String columnTypeName, Integer columnSize) {
        if (!SIMPLE_TYPE_MAPPING.containsKey(jdbcType))
            throw new UnsupportedOperationException("TODO implement edmtype conversion for jdbc type: " + jdbcType);
        return SIMPLE_TYPE_MAPPING.get(jdbcType);
    }
}
