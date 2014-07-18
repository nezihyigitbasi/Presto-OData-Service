package com.presto.odata.sql;

import com.google.common.collect.ImmutableList;
import org.core4j.Enumerable;
import org.junit.Test;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmEntityType;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmType;
import org.odata4j.expression.EntitySimpleProperty;
import org.odata4j.expression.Expression;
import org.odata4j.expression.OrderByExpression;
import org.odata4j.producer.QueryInfo;
import org.odata4j.producer.jdbc.JdbcMetadataMapping;
import org.odata4j.producer.jdbc.JdbcModel;
import org.odata4j.producer.jdbc.SqlStatement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSqlGenerator {

    @Test
    public void testProcessParameters() {
        PrestoSQLGenerator sqlGenerator = new PrestoSQLGenerator();
        StringBuilder query = new StringBuilder("select * from test_table where x=? and y=? and z=?");
        List<SqlStatement.SqlParameter> params = new ArrayList<>();
        params.add(new SqlStatement.SqlParameter(1, Types.INTEGER));
        params.add(new SqlStatement.SqlParameter(2, Types.INTEGER));
        params.add(new SqlStatement.SqlParameter(3, Types.INTEGER));
        String sql = sqlGenerator.processParameters(query, params);
        assertEquals("select * from test_table where x=1 and y=2 and z=3", sql);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testProcessParametersFailure() {
        PrestoSQLGenerator sqlGenerator = new PrestoSQLGenerator();
        StringBuilder query = new StringBuilder("select * from test_table where x=? and y=?");
        List<SqlStatement.SqlParameter> params = new ArrayList<>();
        params.add(new SqlStatement.SqlParameter(1, Types.INTEGER));
        sqlGenerator.processParameters(query, params);
    }

    @Test
    public void testProcessOrderBy() {
        PrestoSQLGenerator sqlGenerator = new PrestoSQLGenerator();

        StringBuilder query = new StringBuilder();
        OrderByExpression orderByExpression = Expression.orderBy(Expression.simpleProperty("test_field"), OrderByExpression.Direction.ASCENDING);
        QueryInfo queryInfo = QueryInfo.newBuilder().setOrderBy(ImmutableList.<OrderByExpression>builder().add(orderByExpression).build()).build();
        sqlGenerator.processOrderBy(queryInfo, query);
        assertEquals(" ORDER BY test_field ASC", query.toString());

        query = new StringBuilder();
        orderByExpression = Expression.orderBy(Expression.simpleProperty("test_field"), OrderByExpression.Direction.DESCENDING);
        queryInfo = QueryInfo.newBuilder().setOrderBy(ImmutableList.<OrderByExpression>builder().add(orderByExpression).build()).build();
        sqlGenerator.processOrderBy(queryInfo, query);
        assertEquals(" ORDER BY test_field DESC", query.toString());
    }

    @Test
    public void testProcessProjections() {
        PrestoSQLGenerator sqlGenerator = new PrestoSQLGenerator();
        StringBuilder query = new StringBuilder();

        JdbcMetadataMapping jdbcMetadataMapping = mock(JdbcMetadataMapping.class);
        JdbcModel.JdbcTable table = mock(JdbcModel.JdbcTable.class);
        EdmEntitySet entitySet = mock(EdmEntitySet.class);

        table.tableName = "test_table";
        when(jdbcMetadataMapping.getMappedTable(any(EdmEntitySet.class))).thenReturn(table);

        EntitySimpleProperty col_1 = Expression.simpleProperty("col_1");
        EntitySimpleProperty col_2 = Expression.simpleProperty("col_2");
        EntitySimpleProperty col_3 = Expression.simpleProperty("col_3");

        QueryInfo queryInfo = QueryInfo.newBuilder().setSelect(ImmutableList.<EntitySimpleProperty>builder().add(col_1).add(col_2).add(col_3).build()).build();
        sqlGenerator.processProjections(jdbcMetadataMapping, entitySet, queryInfo, query);
        assertEquals("col_1 AS col_1,col_2 AS col_2,col_3 AS col_3 FROM test_table", query.toString());
    }

    @Test
    public void testProcessProjectionsAll() {
        //test select all columns
        PrestoSQLGenerator sqlGenerator = new PrestoSQLGenerator();
        StringBuilder query = new StringBuilder();
        QueryInfo queryInfo = QueryInfo.newBuilder().build();

        JdbcMetadataMapping jdbcMetadataMapping = mock(JdbcMetadataMapping.class);
        JdbcModel.JdbcTable table = mock(JdbcModel.JdbcTable.class);
        EdmEntitySet entitySet = mock(EdmEntitySet.class);
        EdmEntityType edmEntityType = mock(EdmEntityType.class);
        EdmType edmType = mock(EdmType.class);

        table.tableName = "test_table";
        when(jdbcMetadataMapping.getMappedTable(any(EdmEntitySet.class))).thenReturn(table);
        when(entitySet.getType()).thenReturn(edmEntityType);

        final EdmProperty prop_1 = EdmProperty.newBuilder("col_1").setType(edmType).build();
        final EdmProperty prop_2 = EdmProperty.newBuilder("col_2").setType(edmType).build();
        final EdmProperty prop_3 = EdmProperty.newBuilder("col_3").setType(edmType).build();
        when(edmEntityType.getProperties()).thenReturn(Enumerable.create(ImmutableList.<EdmProperty>builder().
                add(prop_1).
                add(prop_2).
                add(prop_3).build()));

        JdbcModel.JdbcColumn jdbcColumn_1 = mock(JdbcModel.JdbcColumn.class);
        jdbcColumn_1.columnName = "col_1";
        when(jdbcMetadataMapping.getMappedColumn(prop_1)).thenReturn(jdbcColumn_1);

        JdbcModel.JdbcColumn jdbcColumn_2= mock(JdbcModel.JdbcColumn.class);
        jdbcColumn_2.columnName = "col_2";
        when(jdbcMetadataMapping.getMappedColumn(prop_2)).thenReturn(jdbcColumn_2);

        JdbcModel.JdbcColumn jdbcColumn_3 = mock(JdbcModel.JdbcColumn.class);
        jdbcColumn_3.columnName = "col_3";
        when(jdbcMetadataMapping.getMappedColumn(prop_3)).thenReturn(jdbcColumn_3);

        sqlGenerator.processProjections(jdbcMetadataMapping, entitySet, queryInfo, query);
        assertEquals("col_1 AS col_1,col_2 AS col_2,col_3 AS col_3 FROM test_table", query.toString());
    }
}
