package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.PolicyException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

final class StatementCachingConnection {

    private static final System.Logger LOG = System.getLogger(StatementCachingConnection.class.getName());

    private StatementCachingConnection() {}

    static Connection wrap(final Connection connection) {
        final Map<String, PreparedStatement> cache;
        final InvocationHandler handler;
        cache = new HashMap<>();
        handler = new CacheHandler(connection, cache);
        return (Connection)
                Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, handler);
    }

    static void releaseStatements(final Connection wrapped) {
        if (Proxy.isProxyClass(wrapped.getClass())
                && Proxy.getInvocationHandler(wrapped) instanceof CacheHandler handler) {
            closeAll(handler.cache);
            return;
        }
        throw new IllegalStateException("connection is not wrapped by StatementCachingConnection: " + wrapped);
    }

    private record CacheHandler(Connection connection, Map<String, PreparedStatement> cache)
            implements InvocationHandler {
        @Override
        public Object invoke(Object proxy, final Method method, final Object[] args) throws Throwable {
            return switch (method.getName()) {
                case "prepareStatement" -> prepare(connection, cache, method, args);
                case "close" -> {
                    closeAll(cache);
                    yield StatementCachingConnection.invoke(connection, method, args);
                }
                default -> StatementCachingConnection.invoke(connection, method, args);
            };
        }
    }

    private static Object prepare(
            final Connection connection,
            final Map<String, PreparedStatement> cache,
            final Method method,
            final Object[] args)
            throws Throwable {
        final boolean plain;
        final boolean withKeys;
        final String key;
        PreparedStatement statement;
        plain = args.length == 1;
        withKeys = args.length == 2 && args[1] instanceof Integer;
        if (!plain && !withKeys) {
            return invoke(connection, method, args);
        }
        key = (plain ? "" : args[1] + ":") + args[0];
        statement = cache.get(key);
        if (statement == null) {
            statement = (PreparedStatement) invoke(connection, method, args);
            cache.put(key, statement);
        } else {
            statement.clearParameters();
        }
        return retained(statement);
    }

    private static PreparedStatement retained(final PreparedStatement statement) {
        return (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class<?>[] {PreparedStatement.class},
                (proxy, method, args) -> method.getName().equals("close") ? null : invoke(statement, method, args));
    }

    @PolicyException(
            rule = "PMD.CloseResource",
            reason =
                    "The loop closes each statement retained by the connection cache; failures do not stop other closes.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    private static void closeAll(final Map<String, PreparedStatement> cache) {
        for (final PreparedStatement statement : cache.values()) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.log(System.Logger.Level.WARNING, "failed to close cached SQLite statement", e);
            }
        }
        cache.clear();
    }

    private static Object invoke(final Object target, final Method method, final Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
