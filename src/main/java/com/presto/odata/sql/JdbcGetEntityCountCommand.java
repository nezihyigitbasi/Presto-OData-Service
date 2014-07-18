package com.presto.odata.sql;

import org.core4j.ThrowingFunc1;
import org.odata4j.command.Command;
import org.odata4j.command.CommandResult;
import org.odata4j.core.OEntity;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.exceptions.NotFoundException;
import org.odata4j.producer.CountResponse;
import org.odata4j.producer.command.GetEntitiesCountCommandContext;
import org.odata4j.producer.jdbc.JdbcBaseCommand;
import org.odata4j.producer.jdbc.JdbcMetadataMapping;
import org.odata4j.producer.jdbc.JdbcModel;
import org.odata4j.producer.jdbc.JdbcProducerCommandContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class JdbcGetEntityCountCommand extends JdbcBaseCommand
        implements Command<GetEntitiesCountCommandContext> {
    @Override
    public CommandResult execute(final GetEntitiesCountCommandContext context) throws Exception {
        JdbcProducerCommandContext jdbcContext = (JdbcProducerCommandContext) context;
        String entitySetName = context.getEntitySetName();

        final JdbcMetadataMapping mapping = jdbcContext.getBackend().getMetadataMapping();
        final EdmEntitySet entitySet = mapping.getMetadata().findEdmEntitySet(entitySetName);

        if (entitySet == null)
            throw new NotFoundException();

        //somehow for $count, we don't get anything in queryInfo, so generate SQL here
        JdbcModel.JdbcTable table = mapping.getMappedTable(entitySet);
        final String sqlStatement = "SELECT COUNT(*) AS total FROM " + table.tableName;
        final List<OEntity> entities = new ArrayList<OEntity>();
        jdbcContext.getJdbc().execute(new ThrowingFunc1<Connection, Void>() {
            @Override
            public Void apply(Connection conn) throws Exception {
                final ResultSet results = conn.createStatement().executeQuery(sqlStatement);
                final int count;
                if (results.next())
                    count = results.getInt("total");
                else
                    count = -1;

                context.setResult(new CountResponse() {
                    @Override
                    public long getCount() {
                        return count;
                    }
                });
                return null;
            }
        });
        return CommandResult.CONTINUE;
    }
}
