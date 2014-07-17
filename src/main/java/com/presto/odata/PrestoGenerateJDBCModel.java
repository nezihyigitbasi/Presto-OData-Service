package com.presto.odata;

import org.core4j.ThrowingFunc1;
import org.odata4j.producer.jdbc.JdbcModel;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

/**
 * copied & modified from org.odata4j.producer.jdbc.GenerateJdbcModel
 */
public class PrestoGenerateJDBCModel implements ThrowingFunc1<Connection, JdbcModel> {
    @Override
    public JdbcModel apply(Connection conn) throws Exception {
        JdbcModel model = new JdbcModel();
        DatabaseMetaData meta = conn.getMetaData();

        // schemas
        ResultSet schemas = meta.getSchemas();
        while (schemas.next()) {
            String schemaName = schemas.getString("TABLE_SCHEM");
            JdbcModel.JdbcSchema schema = model.getOrCreateSchema(schemaName);
            schema.catalogName = schemas.getString("TABLE_CATALOG");
            if (schemaName.equals("default"))
                schema.isDefault = true;//schemas.getBoolean("IS_DEFAULT");
            else
                schema.isDefault = false;
            model.getOrCreateSchema(schemaName);
        }

        // tables
        ResultSet tables = meta.getTables(null, null, null, null);
        while (tables.next()) {
            String schemaName = tables.getString("TABLE_SCHEM");
            String tableName = tables.getString("TABLE_NAME");
            JdbcModel.JdbcTable table = model.getOrCreateTable(schemaName, tableName);
            table.tableType = tables.getString("TABLE_TYPE");
            model.getOrCreateTable(schemaName, tableName);
        }

        // columns
        ResultSet columns = meta.getColumns(null, null, null, null);
        while (columns.next()) {
            String schemaName = columns.getString("TABLE_SCHEM");
            String tableName = columns.getString("TABLE_NAME");
            String columnName = columns.getString("COLUMN_NAME");
            JdbcModel.JdbcColumn column = model.getOrCreateColumn(schemaName, tableName, columnName);
            column.columnType = columns.getInt("DATA_TYPE");
            column.columnTypeName = columns.getString("TYPE_NAME");
            column.columnSize = -1;// (Integer) columns.getObject("COLUMN_SIZE");
            column.isNullable = columns.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
            column.ordinalPosition = columns.getInt("ORDINAL_POSITION");
            model.getOrCreateColumn(schemaName, tableName, columnName);

            //add arbitrarily the first col as PK as Presto doesn't support PKs
            JdbcModel.JdbcPrimaryKey primaryKey = new JdbcModel.JdbcPrimaryKey();
            primaryKey.columnName = columnName;
            primaryKey.sequenceNumber = 1;
            primaryKey.primaryKeyName = columnName;
            JdbcModel.JdbcTable t = model.getOrCreateTable(schemaName, tableName);
            if (t.primaryKeys.size() == 0)
                t.primaryKeys.add(primaryKey);
        }

        //java.sql.SQLFeatureNotSupportedException: primary keys not supported
        // primary keys
//        ResultSet primaryKeys = meta.getPrimaryKeys(null, null, null);
//        while (primaryKeys.next()) {
//            String schemaName = primaryKeys.getString("TABLE_SCHEM");
//            String tableName = primaryKeys.getString("TABLE_NAME");
//            JdbcModel.JdbcTable table = model.getTable(schemaName, tableName);
//            JdbcModel.JdbcPrimaryKey primaryKey = new JdbcModel.JdbcPrimaryKey();
//            primaryKey.columnName = primaryKeys.getString("COLUMN_NAME");
//            primaryKey.sequenceNumber = primaryKeys.getInt("KEY_SEQ");
//            primaryKey.primaryKeyName = primaryKeys.getString("PK_NAME");
//            table.primaryKeys.add(primaryKey);
//        }
        return model;
    }

}
