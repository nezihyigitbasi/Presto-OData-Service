package com.presto.odata;

import com.presto.odata.sql.JdbcGetEntityCountCommand;
import com.presto.odata.sql.PrestoJdbcGetEntitiesCommand;
import org.odata4j.command.ChainCommand;
import org.odata4j.command.Command;
import org.odata4j.command.CommandContext;
import org.odata4j.command.CommandExecution;
import org.odata4j.core.OExtension;
import org.odata4j.core.Throwables;
import org.odata4j.producer.ErrorResponseExtension;
import org.odata4j.producer.ErrorResponseExtensions;
import org.odata4j.producer.ODataProducer;
import org.odata4j.producer.QueryInfo;
import org.odata4j.producer.command.CommandProducer;
import org.odata4j.producer.command.CreateEntityCommandContext;
import org.odata4j.producer.command.DeleteEntityCommandContext;
import org.odata4j.producer.command.GetEntitiesCommandContext;
import org.odata4j.producer.command.GetEntitiesCountCommandContext;
import org.odata4j.producer.command.GetEntityCommandContext;
import org.odata4j.producer.command.GetMetadataCommandContext;
import org.odata4j.producer.command.ProducerCommandContext;
import org.odata4j.producer.jdbc.Jdbc;
import org.odata4j.producer.jdbc.JdbcCreateEntityCommand;
import org.odata4j.producer.jdbc.JdbcDeleteEntityCommand;
import org.odata4j.producer.jdbc.JdbcGetEntityCommand;
import org.odata4j.producer.jdbc.JdbcGetMetadataCommand;
import org.odata4j.producer.jdbc.JdbcModel;
import org.odata4j.producer.jdbc.JdbcProducerBackend;
import org.odata4j.producer.jdbc.JdbcProducerBackendInvocationHandler;
import org.odata4j.producer.jdbc.JdbcProducerCommandContext;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
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

    public static class Builder {

        private final Map<Class<?>, Object> instances = new HashMap<Class<?>, Object>();
        private Jdbc jdbc;

        public Builder jdbc(Jdbc jdbc) {
            this.jdbc = jdbc;
            return this;
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

            JdbcProducerBackend jdbcBackend = new JdbcProducerBackend() {

                private <T> Object newContext(Class<?> contextType, Object... args) {
                    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{contextType, JdbcProducerCommandContext.class}, new JdbcProducerBackendInvocationHandler(this, contextType, args));
                }

                public GetEntitiesCountCommandContext newGetEntitiesCountCommandContext(String entitySetName, QueryInfo queryInfo) {
                    return (GetEntitiesCountCommandContext) newContext(GetEntitiesCountCommandContext.class, new Object[]{"entitySetName", entitySetName, "queryInfo", queryInfo});
                }

                @Override
                public <TContext extends CommandContext> Command<TContext> getCommand(Class<TContext> contextType) {
                    ChainCommand.Builder<TContext> chain = ChainCommand.newBuilder();
                    if (GetMetadataCommandContext.class.isAssignableFrom(contextType)) {
                        chain.add(jdbcGetMetadataCommand);//override this
                    } else if (GetEntitiesCommandContext.class.isAssignableFrom(contextType)) {
                        chain.add(new PrestoJdbcGetEntitiesCommand());//override this
                    } else if (GetEntityCommandContext.class.isAssignableFrom(contextType)) {
                        chain.add(new JdbcGetEntityCommand());
                    } else if (CreateEntityCommandContext.class.isAssignableFrom(contextType)) {
                        chain.add(new JdbcCreateEntityCommand());
                    } else if (DeleteEntityCommandContext.class.isAssignableFrom(contextType)) {
                        chain.add(new JdbcDeleteEntityCommand());
                    } else if (GetEntitiesCountCommandContext.class.isAssignableFrom(contextType)) {
                        chain.add(new JdbcGetEntityCountCommand());
                    } else {
                        throw new UnsupportedOperationException("TODO not implemented " + contextType.getSimpleName());
                    }
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
                    return null;
                }

                @Override
                protected <TContext extends CommandContext> List<Command<?>> getPostCommands(Class<TContext> contextType) {
                    return null;
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
