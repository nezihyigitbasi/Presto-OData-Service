package com.presto.odata;

import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntityContainer;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmEntityType;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmSchema;
import org.odata4j.edm.EdmSimpleType;
import org.odata4j.edm.EdmType;
import org.odata4j.producer.jdbc.JdbcMetadataMapping;
import org.odata4j.producer.jdbc.JdbcModel;
import org.odata4j.producer.jdbc.JdbcModelToMetadata;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by nyigitbasi on 7/16/14.
 */
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

    @Override
    public JdbcMetadataMapping apply(JdbcModel jdbcModel) {
        String modelNamespace = getModelNamespace();

        List<EdmEntityType.Builder> entityTypes = new ArrayList<EdmEntityType.Builder>();
        List<EdmEntityContainer.Builder> entityContainers = new ArrayList<EdmEntityContainer.Builder>();
        List<EdmEntitySet.Builder> entitySets = new ArrayList<EdmEntitySet.Builder>();

        Map<EdmEntitySet.Builder, JdbcModel.JdbcTable> entitySetMapping = new HashMap<EdmEntitySet.Builder, JdbcModel.JdbcTable>();
        Map<EdmProperty.Builder, JdbcModel.JdbcColumn> propertyMapping = new HashMap<EdmProperty.Builder, JdbcModel.JdbcColumn>();

        for (JdbcModel.JdbcSchema jdbcSchema : jdbcModel.schemas) {
            for (JdbcModel.JdbcTable jdbcTable : jdbcSchema.tables) {
                if (jdbcTable.primaryKeys.isEmpty()) {
                    System.err.println("Skipping JdbcTable " + jdbcTable.tableName + ", no keys");
                    continue;
                }

                String entityTypeName = getEntityTypeName(jdbcTable.tableName);
                EdmEntityType.Builder entityType = EdmEntityType.newBuilder()
                        .setName(entityTypeName)
                        .setNamespace(modelNamespace);
                entityTypes.add(entityType);

                for (JdbcModel.JdbcPrimaryKey primaryKey : jdbcTable.primaryKeys) {
                    String propertyName = getPropertyName(primaryKey.columnName);
                    entityType.addKeys(propertyName);
                }

                for (JdbcModel.JdbcColumn jdbcColumn : jdbcTable.columns) {
                    String propertyName = getPropertyName(jdbcColumn.columnName);
                    EdmType propertyType = getEdmType(jdbcColumn.columnType, jdbcColumn.columnTypeName, jdbcColumn.columnSize);
                    EdmProperty.Builder property = EdmProperty.newBuilder(propertyName)
                            .setType(propertyType)
                            .setNullable(jdbcColumn.isNullable);
                    entityType.addProperties(property);
                    propertyMapping.put(property, jdbcColumn);
                }

                String entitySetName = getEntitySetName(jdbcTable.tableName);
                EdmEntitySet.Builder entitySet = EdmEntitySet.newBuilder()
                        .setName(entitySetName)
                        .setEntityType(entityType);
                entitySets.add(entitySet);
                entitySetMapping.put(entitySet, jdbcTable);
            }

            String entityContainerName = getEntityContainerName(jdbcSchema.schemaName);
            EdmEntityContainer.Builder entityContainer = EdmEntityContainer.newBuilder()
                    .setName(entityContainerName)
                    .setIsDefault(jdbcSchema.isDefault)
                    .addEntitySets(entitySets);
            entityContainers.add(entityContainer);
        }

        List<EdmSchema.Builder> edmSchemas = new ArrayList<EdmSchema.Builder>();
        EdmSchema.Builder modelSchema = EdmSchema.newBuilder()
                .setNamespace(modelNamespace)
                .addEntityTypes(entityTypes);
        edmSchemas.add(modelSchema);
        for (EdmEntityContainer.Builder entityContainer : entityContainers) {
            String containerSchemaNamespace = getContainerNamespace(entityContainer.getName());
            EdmSchema.Builder containerSchema = EdmSchema.newBuilder()
                    .setNamespace(containerSchemaNamespace)
                    .addEntityContainers(entityContainer);
            edmSchemas.add(containerSchema);
        }
        EdmDataServices metadata = EdmDataServices.newBuilder()
                .addSchemas(edmSchemas)
                .build();

        Map<EdmEntitySet, JdbcModel.JdbcTable> finalEntitySetMapping = new HashMap<EdmEntitySet, JdbcModel.JdbcTable>();
        for (Map.Entry<EdmEntitySet.Builder, JdbcModel.JdbcTable> entry : entitySetMapping.entrySet()) {
            finalEntitySetMapping.put(entry.getKey().build(), entry.getValue());
        }
        Map<EdmProperty, JdbcModel.JdbcColumn> finalPropertyMapping = new HashMap<EdmProperty, JdbcModel.JdbcColumn>();
        for (Map.Entry<EdmProperty.Builder, JdbcModel.JdbcColumn> entry : propertyMapping.entrySet()) {
            finalPropertyMapping.put(entry.getKey().build(), entry.getValue());
        }
        return new JdbcMetadataMapping(metadata, jdbcModel, finalEntitySetMapping, finalPropertyMapping);
    }
}
