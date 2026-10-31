package io.github.ppzxc.smq.storage.sqlite;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 준비한 문장을 SQL 문자열마다 하나씩 붙들어 두는 {@link Connection}. 연산마다 문장을 새로 준비하고 해제하는
 * 비용이 단건 처리량의 큰 몫이어서 둔다. 문장 {@code close()}는 바인딩만 비우고 문장을 살려 두며, 연결을
 * 닫을 때 한꺼번에 해제한다.
 *
 * <p>동시에 쓰면 안 된다. 연결이 하나이고 {@link SqliteStorage}가 락으로 직렬화하므로 거기서는 그대로 성립한다.
 * 호출자는 쓴 문장의 {@code ResultSet}을 닫아야 한다(문장이 살아 있으므로 열린 채 두면 읽기 트랜잭션이 남는다).
 */
final class StatementCachingConnection {

    private StatementCachingConnection() {}

    static Connection wrap(Connection connection) {
        Map<String, PreparedStatement> cache = new HashMap<>();
        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "prepareStatement" -> prepare(connection, cache, method, args);
            case "close" -> {
                closeAll(cache);
                yield invoke(connection, method, args);
            }
            default -> invoke(connection, method, args);
        };
        return (Connection)
                Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, handler);
    }

    /** 캐시하는 것은 {@code prepareStatement(sql)}과 {@code prepareStatement(sql, keys)}뿐이다. 나머지 형태는 그대로 넘긴다. */
    private static Object prepare(
            Connection connection, Map<String, PreparedStatement> cache, Method method, Object[] args)
            throws Throwable {
        boolean plain = args.length == 1;
        boolean withKeys = args.length == 2 && args[1] instanceof Integer;
        if (!plain && !withKeys) {
            return invoke(connection, method, args);
        }
        String key = (plain ? "" : args[1] + ":") + args[0];
        PreparedStatement statement = cache.get(key);
        if (statement == null) {
            statement = (PreparedStatement) invoke(connection, method, args);
            cache.put(key, statement);
        } else {
            statement.clearParameters();
        }
        return retained(statement);
    }

    /** {@code close()}를 바인딩 비우기로 바꾼 문장 뷰. */
    private static PreparedStatement retained(PreparedStatement statement) {
        return (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class<?>[] {PreparedStatement.class},
                (proxy, method, args) -> method.getName().equals("close") ? null : invoke(statement, method, args));
    }

    private static void closeAll(Map<String, PreparedStatement> cache) {
        for (PreparedStatement statement : cache.values()) {
            try {
                statement.close();
            } catch (SQLException ignored) {
                // 닫기 실패는 무시한다. 연결을 닫으면 어차피 정리된다.
            }
        }
        cache.clear();
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
