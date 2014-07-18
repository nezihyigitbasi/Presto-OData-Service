package com.presto.odata.sql;

import static com.google.common.base.Preconditions.*;

import com.google.common.base.CharMatcher;
import com.presto.odata.sql.visitor.OrderByVisitor;
import org.core4j.Enumerable;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmProperty;
import org.odata4j.expression.EntitySimpleProperty;
import org.odata4j.expression.OrderByExpression;
import org.odata4j.producer.QueryInfo;
import org.odata4j.producer.jdbc.GenerateWhereClause;
import org.odata4j.producer.jdbc.JdbcMetadataMapping;
import org.odata4j.producer.jdbc.JdbcModel;
import org.odata4j.producer.jdbc.SqlStatement;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PrestoSQLGenerator {

    public String generate(JdbcMetadataMapping mapping, EdmEntitySet entitySet, QueryInfo queryInfo) {
        StringBuilder sqlQuery = new StringBuilder("SELECT ");

        //process projection
        processProjections(mapping, entitySet, queryInfo, sqlQuery);

        //process filter
        List<SqlStatement.SqlParameter> params = processFilter(mapping, entitySet, queryInfo, sqlQuery);

        //process order by
        processOrderBy(queryInfo, sqlQuery);

        //process parameters
        return processParameters(sqlQuery, params);
    }

    protected String processParameters(StringBuilder sqlQuery, List<SqlStatement.SqlParameter> params) {
        String sql = sqlQuery.toString();
        int placeHolderCount = CharMatcher.is('?').countIn(sql);
        checkArgument(placeHolderCount == params.size(), "Number of placeholders does not match the given number of parameters");

        Iterator<SqlStatement.SqlParameter> paramIter = params.iterator();
        //TODO look at all parameter types
        while (paramIter.hasNext()) {
            SqlStatement.SqlParameter p = paramIter.next();
            switch (p.sqlType) {
                case Types.INTEGER:
                    sql = sql.replaceFirst("\\?", p.value.toString());
                    break;
                case Types.VARCHAR:
                    sql = sql.replaceFirst("\\?", "'" + p.value.toString() + "'");
                    break;
                default:
                    throw new RuntimeException("not implemented for type " + p.sqlType);
            }
            paramIter.remove();
        }
        return sql;
    }

    protected void processOrderBy(QueryInfo queryInfo, StringBuilder sqlQuery) {
        if (queryInfo.orderBy != null) {
            List<OrderByExpression> orderByExpressions = queryInfo.orderBy;
            OrderByVisitor orderByVisitor = new OrderByVisitor();
            for (OrderByExpression orderByExpression : orderByExpressions) {
                orderByExpression.visit(orderByVisitor);
                orderByVisitor.append(sqlQuery);
            }
        }
    }

    protected List<SqlStatement.SqlParameter> processFilter(JdbcMetadataMapping mapping, EdmEntitySet entitySet, QueryInfo queryInfo, StringBuilder sqlQuery) {
        List<SqlStatement.SqlParameter> params = new ArrayList<>();
        if (queryInfo.filter != null) {
            GenerateWhereClause whereClauseGen = newWhereClauseGenerator(entitySet, mapping);
            queryInfo.filter.visit(whereClauseGen);
            whereClauseGen.append(sqlQuery, params);
        }
        return params;
    }

    protected void processProjections(JdbcMetadataMapping mapping, EdmEntitySet entitySet, QueryInfo queryInfo, StringBuilder sqlQuery) {
        JdbcModel.JdbcTable table = mapping.getMappedTable(entitySet);

        final int size = queryInfo.select.size();
        if (size > 0) {
            for (int i = 0; i < size; i++) {
                EntitySimpleProperty p = queryInfo.select.get(i);
                StringBuilder sb = new StringBuilder();
                sqlQuery.append(p.getPropertyName()).append(" AS ").append(p.getPropertyName());
                if (i < size - 1) {
                    sqlQuery.append(",");
                }
            }
        } else {
            //explicitly label the columns as we use the labels in the result set later
            final Enumerable<EdmProperty> properties = entitySet.getType().getProperties();
            int count = properties.count();
            int i = 0;
            for (EdmProperty edmProperty : properties) {
                JdbcModel.JdbcColumn column = mapping.getMappedColumn(edmProperty);
                sqlQuery.append(column.columnName).append(" AS ").append(column.columnName);
                if (i++ < count - 1) {
                    sqlQuery.append(",");
                }
            }
        }
        sqlQuery.append(" FROM ").append(table.tableName);
    }

    public GenerateWhereClause newWhereClauseGenerator(EdmEntitySet entitySet, JdbcMetadataMapping mapping) {
        return new GenerateWhereClause(entitySet, mapping);
    }
}
