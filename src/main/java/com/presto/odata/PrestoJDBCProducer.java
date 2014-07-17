package com.presto.odata;

import org.core4j.ThrowingFunc1;
import org.odata4j.command.ChainCommand;
import org.odata4j.command.Command;
import org.odata4j.command.CommandContext;
import org.odata4j.command.CommandExecution;
import org.odata4j.command.CommandResult;
import org.odata4j.core.OEntity;
import org.odata4j.core.OExtension;
import org.odata4j.core.Throwables;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.exceptions.NotFoundException;
import org.odata4j.expression.BoolCommonExpression;
import org.odata4j.producer.EntitiesResponse;
import org.odata4j.producer.ErrorResponseExtension;
import org.odata4j.producer.ErrorResponseExtensions;
import org.odata4j.producer.ODataProducer;
import org.odata4j.producer.Responses;
import org.odata4j.producer.command.CloseCommandContext;
import org.odata4j.producer.command.CommandProducer;
import org.odata4j.producer.command.CreateEntityCommandContext;
import org.odata4j.producer.command.DeleteEntityCommandContext;
import org.odata4j.producer.command.GetEntitiesCommandContext;
import org.odata4j.producer.command.GetEntityCommandContext;
import org.odata4j.producer.command.GetMetadataCommandContext;
import org.odata4j.producer.command.ProducerCommandContext;
import org.odata4j.producer.jdbc.GenerateSqlQuery;
import org.odata4j.producer.jdbc.Jdbc;
import org.odata4j.producer.jdbc.JdbcCreateEntityCommand;
import org.odata4j.producer.jdbc.JdbcDeleteEntityCommand;
import org.odata4j.producer.jdbc.JdbcGetEntitiesCommand;
import org.odata4j.producer.jdbc.JdbcGetEntityCommand;
import org.odata4j.producer.jdbc.JdbcGetMetadataCommand;
import org.odata4j.producer.jdbc.JdbcMetadataMapping;
import org.odata4j.producer.jdbc.JdbcModel;
import org.odata4j.producer.jdbc.JdbcProducerBackend;
import org.odata4j.producer.jdbc.JdbcProducerCommandContext;
import org.odata4j.producer.jdbc.SqlStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * copied & modified from org.odata4j.producer.jdbc.JdbcProducer
 */
public class PrestoJDBCProducer extends CommandProducer {


    private final JdbcProducerBackend jdbcBackend;

    protected PrestoJDBCProducer(JdbcProducerBackend jdbcBackend) {
        super(jdbcBackend);
        this.jdbcBackend = jdbcBackend;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public <TExtension extends OExtension<ODataProducer>> TExtension findExtension(Class<TExtension> clazz) {
        if (ErrorResponseExtension.class.equals(clazz))
            return (TExtension) ErrorResponseExtensions.returnInnerErrorsBasedOnDefaultSystemProperty();
        return super.findExtension(clazz);
    }

    public Jdbc getJdbc() {
        return jdbcBackend.getJdbc();
    }

    public static class Builder {

        private final Map<Class<?>, Object> instances = new HashMap<Class<?>, Object>();
        private final Map<Class<?>, List<Command<?>>> preCommands = new HashMap<Class<?>, List<Command<?>>>();
        private final Map<Class<?>, List<Command<?>>> postCommands = new HashMap<Class<?>, List<Command<?>>>();

        private Jdbc jdbc;

        public Builder jdbc(Jdbc jdbc) {
            this.jdbc = jdbc;
            return this;
        }

        public <TContext extends ProducerCommandContext<?>> Builder insert(Class<TContext> contextType, Command<?> command) {
            return preOrPost(contextType, command, preCommands);
        }

        public <TContext extends ProducerCommandContext<?>> Builder append(Class<TContext> contextType, Command<?> command) {
            return preOrPost(contextType, command, postCommands);
        }

        private <TContext extends ProducerCommandContext<?>> Builder preOrPost(Class<TContext> contextType, Command<?> command,
                                                                               Map<Class<?>, List<Command<?>>> map) {
            if (!map.containsKey(contextType))
                map.put(contextType, new ArrayList<Command<?>>());
            map.get(contextType).add(command);
            return this;
        }

        public PrestoJDBCProducer build() {
            if (jdbc == null)
                throw new IllegalArgumentException("Jdbc is mandatory");

            final JdbcGetMetadataCommand jdbcGetMetadataCommand = new JdbcGetMetadataCommand() {
                @Override
                public JdbcModel generateModel(JdbcProducerCommandContext jdbcContext) {
                    return jdbcContext.getJdbc().execute(new PrestoGenerateJDBCModel());
                }

                @Override
                public void cleanupModel(JdbcModel model) {
                    //nop
                }
            };

            final JdbcGetEntitiesCommand jdbcGetEntitiesCommand = new JdbcGetEntitiesCommand() {
                @Override
                public CommandResult execute(GetEntitiesCommandContext context) throws Exception {
                    JdbcProducerCommandContext jdbcContext = (JdbcProducerCommandContext) context;

                    String entitySetName = context.getEntitySetName();

                    final JdbcMetadataMapping mapping = jdbcContext.getBackend().getMetadataMapping();
                    final EdmEntitySet entitySet = mapping.getMetadata().findEdmEntitySet(entitySetName);
                    if (entitySet == null)
                        throw new NotFoundException();

                    GenerateSqlQuery queryGen = jdbcContext.get(GenerateSqlQuery.class);
                    BoolCommonExpression filter = context.getQueryInfo() == null ? null : context.getQueryInfo().filter;
                    final SqlStatement sqlStatement = queryGen.generate(mapping, entitySet, filter);
                    final List<OEntity> entities = new ArrayList<OEntity>();

                    jdbcContext.getJdbc().execute(new ThrowingFunc1<Connection, Void>() {
                        @Override
                        public Void apply(Connection conn) throws Exception {

                            List<SqlStatement.SqlParameter> params = new ArrayList<SqlStatement.SqlParameter>();
                            for (SqlStatement.SqlParameter p : sqlStatement.params) {
                                params.add(p);
                            }

                            Iterator<SqlStatement.SqlParameter> piter = params.iterator();
                            String sql = sqlStatement.sql;
                            //TODO look at parameter's sqlType to generate the right query
                            while (piter.hasNext()) {
                                SqlStatement.SqlParameter p = piter.next();
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
                                piter.remove();
                            }

                            ResultSet results = conn.createStatement().executeQuery(sql);
//                                            PreparedStatement stmt = sqlStatement.asPreparedStatement(conn);
//                                            ResultSet results = stmt.executeQuery();
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
            };

            JdbcProducerBackend jdbcBackend = new JdbcProducerBackend() {

                @Override
                public <TContext extends CommandContext> Command<TContext> getCommand(Class<TContext> contextType) {
                    ChainCommand.Builder<TContext> chain = ChainCommand.newBuilder();
                    chain.addAll(getPreCommands(ProducerCommandContext.class));
                    if (CloseCommandContext.class.isAssignableFrom(contextType)) {
                        chain.addAll(getPreCommands(CloseCommandContext.class));
                        chain.addAll(getPostCommands(CloseCommandContext.class));
                    } else if (GetMetadataCommandContext.class.isAssignableFrom(contextType)) {
                        chain.addAll(getPreCommands(GetMetadataCommandContext.class));
                        chain.add(jdbcGetMetadataCommand);//override this
                        chain.addAll(getPostCommands(GetMetadataCommandContext.class));
                    } else if (GetEntitiesCommandContext.class.isAssignableFrom(contextType)) {
                        chain.addAll(getPreCommands(GetEntitiesCommandContext.class));
                        chain.add(jdbcGetEntitiesCommand);//override this
                        chain.addAll(getPostCommands(GetEntitiesCommandContext.class));
                    } else if (GetEntityCommandContext.class.isAssignableFrom(contextType)) {
                        chain.addAll(getPreCommands(GetEntityCommandContext.class));
                        chain.add(new JdbcGetEntityCommand());
                        chain.addAll(getPostCommands(GetEntityCommandContext.class));
                    } else if (CreateEntityCommandContext.class.isAssignableFrom(contextType)) {
                        chain.addAll(getPreCommands(CreateEntityCommandContext.class));
                        chain.add(new JdbcCreateEntityCommand());
                        chain.addAll(getPostCommands(CreateEntityCommandContext.class));
                    } else if (DeleteEntityCommandContext.class.isAssignableFrom(contextType)) {
                        chain.addAll(getPreCommands(DeleteEntityCommandContext.class));
                        chain.add(new JdbcDeleteEntityCommand());
                        chain.addAll(getPostCommands(DeleteEntityCommandContext.class));
                    } else {
                        throw new UnsupportedOperationException("TODO implement: " + contextType.getSimpleName());
                    }
                    chain.addAll(getPostCommands(ProducerCommandContext.class));
                    return chain.build();
                }


                @Override
                public CommandExecution getCommandExecution() {
                    return CommandExecution.DEFAULT;
                }

                @Override
                public Jdbc getJdbc() {
                    return jdbc;
                }

                @Override
                protected <TContext extends CommandContext> List<Command<?>> getPreCommands(Class<TContext> contextType) {
                    return preCommands.get(contextType);
                }

                @Override
                protected <TContext extends CommandContext> List<Command<?>> getPostCommands(Class<TContext> contextType) {
                    return postCommands.get(contextType);
                }

                @SuppressWarnings("unchecked")
                @Override
                protected <T> T get(Class<T> instanceType) {
                    Object rt = instances.get(instanceType);
                    if (rt == null) {
                        try {
                            rt = instanceType.newInstance();
                        } catch (Exception e) {
                            throw Throwables.propagate(e);
                        }
                    }
                    return (T) rt;
                }

            };
            return new PrestoJDBCProducer(jdbcBackend);
        }

        public <T> Builder register(Class<T> instanceType, T instance) {
            instances.put(instanceType, instance);
            return this;
        }

    }

}
