package com.presto.odata.sql;

import org.core4j.ThrowingFunc1;
import org.odata4j.command.CommandResult;
import org.odata4j.core.OEntities;
import org.odata4j.core.OEntity;
import org.odata4j.core.OEntityKey;
import org.odata4j.core.OLink;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.exceptions.NotFoundException;
import org.odata4j.producer.EntitiesResponse;
import org.odata4j.producer.Responses;
import org.odata4j.producer.command.GetEntitiesCommandContext;
import org.odata4j.producer.jdbc.JdbcMetadataMapping;
import org.odata4j.producer.jdbc.JdbcProducerCommandContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PrestoJdbcGetEntitiesCommand extends org.odata4j.producer.jdbc.JdbcGetEntitiesCommand {
    @Override
    public CommandResult execute(GetEntitiesCommandContext context) throws Exception {
        JdbcProducerCommandContext jdbcContext = (JdbcProducerCommandContext) context;

        String entitySetName = context.getEntitySetName();

        final JdbcMetadataMapping mapping = jdbcContext.getBackend().getMetadataMapping();
        final EdmEntitySet entitySet = mapping.getMetadata().findEdmEntitySet(entitySetName);
        if (entitySet == null)
            throw new NotFoundException();

        PrestoSQLGenerator queryGen = new PrestoSQLGenerator();
        final String sqlStatement = queryGen.generate(mapping, entitySet, context.getQueryInfo());
        final List<OEntity> entities = new ArrayList<OEntity>();
        jdbcContext.getJdbc().execute(new ThrowingFunc1<Connection, Void>() {
            @Override
            public Void apply(Connection conn) throws Exception {
                ResultSet results = conn.createStatement().executeQuery(sqlStatement);
                while (results.next()) {
                    OEntity entity = toOEntity(mapping, entitySet, results);
                    entities.add(entity);
                }
                return null;
            }
        });

        Integer inlineCount = null;
        String skipToken = null;
        EntitiesResponse response = Responses.entities(entities, entitySet, inlineCount, skipToken);
        context.setResult(response);
        return CommandResult.CONTINUE;
    }

    protected OEntity toOEntity(JdbcMetadataMapping mapping, EdmEntitySet entitySet, ResultSet results) throws SQLException {
        List<OProperty<?>> properties = new ArrayList<OProperty<?>>();
        int colCount = results.getMetaData().getColumnCount();
        for (int i = 1; i <= colCount; i++) {
            String colLabel = results.getMetaData().getColumnLabel(i);
            Object value = results.getObject(colLabel);
            OProperty<?> property = OProperties.simple(colLabel, value);
            properties.add(property);
        }
        return OEntities.create(entitySet, OEntityKey.create("test"), properties, Collections.<OLink>emptyList());
    }
}
