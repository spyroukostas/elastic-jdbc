package gr.uoa.di.madgik.elastic;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;

import javax.net.ssl.SSLContext;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.stream.Collectors;


public class JdbcConnection extends JdbcWrapper implements Connection {

    private static final Logger logger = Logger.getLogger("gr.madgeek.elastic.Driver");

    private static final String NUM_SERVERS = "numServers";
    private static final String PREFIX_SERVER = "server";

    private final ReentrantLock lock = new ReentrantLock();

    private final RestClient client;
    private Map<String, Class<?>> typeMap = new HashMap<>();

    private String catalog = null;
    private boolean autoCommit = true;

    private int holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;

    private Map<String, ClientInfoStatus> clientInfo;

    public JdbcConnection(String url, Properties properties) throws SQLException {
        try {
            // TODO: refactor
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            if (properties.containsKey("user") && properties.containsKey("password")) {
                String username = properties.getProperty("user");
                String password = properties.getProperty("password");
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            }

            String elasticUrl = url.split("//", 2)[1]; // get url without jdbc// prefix
            elasticUrl = elasticUrl.startsWith("http") ? elasticUrl : "http://" + elasticUrl;
            HttpHost httpHost = HttpHost.create(elasticUrl);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, null, null);

            RestClient restClient = RestClient.builder(httpHost)
                    .setHttpClientConfigCallback(config -> config
                            .setDefaultCredentialsProvider(credentialsProvider)
                            .setSSLContext(sslContext)
                            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    )
                    .build();

            logger.info("Connected to: " + url);
            client = restClient;

        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * @return Elasticsearch rest client.
     */
    public RestClient getClient() {
        return client;
    }

    /**
     * Locks this connection with a reentrant lock.
     *
     * <pre>
     * lock();
     * try {
     *     ...
     * } finally {
     *     unlock();
     * }
     * </pre>
     */
    protected final void lock() {
        lock.lock();
    }

    /**
     * Unlocks this connection.
     *
     * @see #lock()
     */
    protected final void unlock() {
        lock.unlock();
    }

    /**
     * Checks whether the connection is closed.
     *
     * @throws SQLException if the connection is closed
     */
    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection has been closed.");
        }
    }

    /**
     * Creates a new statement.
     *
     * @return the new statement
     * @throws SQLException if the connection is closed
     */
    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new JdbcStatement(this);
    }

    /**
     * Creates a statement with the specified result set type and concurrency.
     *
     * @param resultSetType        the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @return the statement
     * @throws SQLException if the connection is closed or the result set type
     *                      or concurrency are not supported
     */
    @Override
    public Statement createStatement(int resultSetType,
                                     int resultSetConcurrency) throws SQLException {
        checkClosed();
        return new JdbcStatement(this, resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    /**
     * Creates a statement with the specified result set type, concurrency, and
     * holdability.
     *
     * @param resultSetType        the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @param resultSetHoldability the holdability (ResultSet.HOLD* / CLOSE*)
     * @return the statement
     * @throws SQLException if the connection is closed or the result set type,
     *                      concurrency, or holdability are not supported
     */
    @Override
    public Statement createStatement(int resultSetType,
                                     int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        checkClosed();
        return new JdbcStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    /**
     * Creates a new prepared statement.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     * @throws SQLException if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new JdbcPreparedStatement(this, nativeSQL(sql));
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @return the database meta data
     * @throws SQLException if the connection is closed
     */
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * Closes this connection. All open statements, prepared statements and
     * result sets that where created by this connection become invalid after
     * calling this method.
     *
     * @throws SQLException if there is a problem when closing the connection
     */
    @Override
    public void close() throws SQLException {
        lock();
        try {
            client.close();
        } catch (Throwable e) {
            throw logAndConvert(e);
        } finally {
            unlock();
        }
    }

    /**
     * Switches auto commit on or off.
     *
     * @param autoCommit true for auto commit on, false for off
     * @throws SQLException if the connection is closed
     */
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        try {
            if (!autoCommit) {
                throw new SQLFeatureNotSupportedException("Cannot disable auto-commit");
            }
            this.autoCommit = autoCommit;
        } catch (Exception e) {
            throw logAndConvert(e);
        } finally {
            unlock();
        }
    }

    /**
     * Gets the current setting for auto commit.
     *
     * @return true for on, false for off
     * @throws SQLException if the connection is closed
     */
    @Override
    public boolean getAutoCommit() throws SQLException {
        lock();
        try {
            return autoCommit;
        } catch (Exception e) {
            throw logAndConvert(e);
        } finally {
            unlock();
        }
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @throws SQLException if the connection is closed
     */
    @Override
    public void commit() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @throws SQLException if the connection is closed
     */
    @Override
    public void rollback() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * Returns true if this connection has been closed.
     *
     * @return true if close was called
     */
    @Override
    public boolean isClosed() {
        return client == null;
    }

    /**
     * Translates a SQL statement into Elasticsearch SQL grammar.
     *
     * @param sql an SQL statement that may contain one or more '?'
     *            parameter placeholders
     * @return
     * @throws SQLException if the connection is closed
     */
    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        // TODO: refactor
        return sql.endsWith(";") ? sql.substring(0, sql.length() - 1) : sql;
    }

    /**
     * According to the JDBC specs, this setting is only a hint to the database
     * to enable optimizations - it does not cause writes to be prohibited.
     *
     * @param readOnly ignored
     * @throws SQLException if the connection is closed
     */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
    }

    /**
     * Returns true if the database is read-only.
     *
     * @return if the database is read-only
     * @throws SQLException if the connection is closed
     */
    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return true;
    }

    /**
     * Set the default catalog name. This call is ignored.
     *
     * @param catalog ignored
     * @throws SQLException if the connection is closed
     */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        this.catalog = catalog;
    }

    /**
     * Gets the current catalog name.
     *
     * @return the catalog name
     * @throws SQLException if the connection is closed
     */
    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return this.catalog;
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     * <p>
     * Gets the first warning reported by calls on this object.
     *
     * @return null
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("getWarnings() not implemented");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     * <p>
     * Clears all warnings.
     */
    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        // no-op
    }

    /**
     * Creates a prepared statement with the specified result set type and
     * concurrency.
     *
     * @param sql                  the SQL statement
     * @param resultSetType        the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @return the prepared statement
     * @throws SQLException if the connection is closed or the result set type
     *                      or concurrency are not supported
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        checkClosed();
        return new JdbcPreparedStatement(this, nativeSQL(sql), resultSetType, resultSetConcurrency, holdability);
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @param level one of the following {@code Connection} constants:
     *              {@code Connection.TRANSACTION_READ_UNCOMMITTED},
     *              {@code Connection.TRANSACTION_READ_COMMITTED},
     *              {@code Connection.TRANSACTION_REPEATABLE_READ}, or
     *              {@code Connection.TRANSACTION_SERIALIZABLE}.
     *              (Note that {@code Connection.TRANSACTION_NONE} cannot be used
     *              because it specifies that transactions are not supported.)
     * @throws SQLException if the connection is closed
     */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * Returns the current transaction isolation level.
     *
     * @return the isolation level
     * @throws SQLException if the connection is closed
     */
    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return Connection.TRANSACTION_NONE;
    }

    /**
     * Changes the current result set holdability.
     *
     * @param holdability ResultSet.HOLD_CURSORS_OVER_COMMIT or
     *                    ResultSet.CLOSE_CURSORS_AT_COMMIT;
     * @throws SQLException if the connection is closed or the holdability is
     *                      not supported
     */
    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException("");
        }
        this.holdability = holdability;
    }

    /**
     * Returns the current result set holdability.
     *
     * @return the holdability
     * @throws SQLException if the connection is closed
     */
    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return holdability;
    }

    /**
     * Gets the type map.
     *
     * @return the type map
     * @throws SQLException if the connection is closed
     */
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkClosed();
        return typeMap;
    }

    /**
     * Sets the type map.
     *
     * @param map the {@code java.util.Map} object to install
     *            as the replacement for this {@code Connection}
     *            object's default type map
     * @throws SQLException if the connection is closed
     */
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        this.typeMap = map;
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     * <p>
     * Creates a new callable statement.
     *
     * @param sql the SQL statement
     * @return the callable statement
     * @throws SQLException if the connection is closed or the statement is not
     *                      valid
     */
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     * <p>
     * Creates a callable statement with the specified result set type and
     * concurrency.
     *
     * @param sql                  the SQL statement
     * @param resultSetType        the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @return the callable statement
     * @throws SQLException if the connection is closed or the result set type
     *                      or concurrency are not supported
     */
    @Override
    public CallableStatement prepareCall(String sql,
                                         int resultSetType,
                                         int resultSetConcurrency) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * Creates a callable statement with the specified result set type,
     * concurrency, and holdability.
     *
     * @param sql                  the SQL statement
     * @param resultSetType        the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @param resultSetHoldability the holdability (ResultSet.HOLD* / CLOSE*)
     * @return the callable statement
     * @throws SQLException if the connection is closed or the result set type,
     *                      concurrency, or holdability are not supported
     */
    @Override
    public CallableStatement prepareCall(String sql,
                                         int resultSetType,
                                         int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @return the new savepoint
     * @throws SQLException if the connection is closed
     */
    @Override
    public Savepoint setSavepoint() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Savepoint not supported");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @param name the savepoint name
     * @return the new savepoint
     * @throws SQLException if the connection is closed
     */
    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Savepoint not supported");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @param savepoint the savepoint
     * @throws SQLException if the connection is closed
     */
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Rollbacks not supported");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @param savepoint the savepoint to release
     * @throws SQLException if the connection is closed
     */
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * Creates a prepared statement with the specified result set type,
     * concurrency, and holdability.
     *
     * @param sql                  the SQL statement
     * @param resultSetType        the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @param resultSetHoldability the holdability (ResultSet.HOLD* / CLOSE*)
     * @return the prepared statement
     * @throws SQLException if the connection is closed or the result set type,
     *                      concurrency, or holdability are not supported
     */
    @Override
    public PreparedStatement prepareStatement(String sql,
                                              int resultSetType,
                                              int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        checkClosed();
        return new JdbcPreparedStatement(this, nativeSQL(sql), resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     * <p>
     * Creates a new prepared statement.
     *
     * @param sql               the SQL statement
     * @param autoGeneratedKeys {@link Statement#RETURN_GENERATED_KEYS} if generated keys should
     *                          be available for retrieval, {@link Statement#NO_GENERATED_KEYS} if
     *                          generated keys should not be available
     * @return the prepared statement
     * @throws SQLException if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     * <p>
     * Creates a new prepared statement.
     *
     * @param sql           the SQL statement
     * @param columnIndexes an array of column indexes indicating the columns with generated
     *                      keys that should be returned from the inserted row
     * @return the prepared statement
     * @throws SQLException if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     * <p>
     * Creates a new prepared statement.
     *
     * @param sql         the SQL statement
     * @param columnNames an array of column names indicating the columns with generated
     *                    keys that should be returned from the inserted row
     * @return the prepared statement
     * @throws SQLException if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @throws SQLException if the connection is closed
     */
    @Override
    public Clob createClob() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @throws SQLException if the connection is closed
     */
    @Override
    public Blob createBlob() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @throws SQLException if the connection is closed
     */
    @Override
    public NClob createNClob() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @throws SQLException if the connection is closed
     */
    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @param typeName the SQL name of the type the elements of the array map to. The typeName is a
     *                 database-specific name which may be the name of a built-in type, a user-defined type or a standard  SQL type supported by this database. This
     *                 is the value returned by {@code Array.getBaseTypeName}
     * @param elements the elements that populate the returned object
     * @throws SQLException if the connection is closed
     */
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @param typeName   the SQL type name of the SQL structured type that this {@code Struct}
     *                   object maps to. The typeName is the name of  a user-defined type that
     *                   has been defined for this database. It is the value returned by
     *                   {@code Struct.getSQLTypeName}.
     * @param attributes the attributes that populate the returned object
     * @throws SQLException if the connection is closed
     */
    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * Returns true if this connection is still valid.
     *
     * @param timeout the number of seconds to wait for the database to respond
     *                (ignored)
     * @return true if the connection is valid.
     */
    @Override
    public boolean isValid(int timeout) {
        lock();
        try {
            if (client == null) {
                return false;
            }
            return client.getNodes() != null;
        } catch (Exception e) {
            return false;
        } finally {
            unlock();
        }
    }

    /**
     * Set a client property.
     * For unsupported properties a SQLClientInfoException is thrown.
     *
     * @param name  the name of the property
     * @param value the value
     */
    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            if (clientInfo == null) {
                clientInfo = new HashMap<>();
            }
            if (Objects.equals(value, getClientInfo(name))) {
                return;
            }
            clientInfo.put(name, ClientInfoStatus.valueOf(value));

        } catch (Exception e) {
            throw new SQLClientInfoException(e.getMessage(), clientInfo, logAndConvert(e));
        }
    }

    private static SQLClientInfoException convertToClientInfoException(SQLException x) {
        if (x instanceof SQLClientInfoException) {
            return (SQLClientInfoException) x;
        }
        return new SQLClientInfoException(x.getMessage(), x.getSQLState(),
                x.getErrorCode(), null, null);
    }

    /**
     * Set the client properties. This replaces all existing properties. This
     * method always throws a SQLClientInfoException in standard mode. In
     * compatibility mode some properties may be supported (see
     * setProperty(String, String) for details).
     *
     * @param properties the list of client info properties to set
     * @throws SQLClientInfoException
     */
    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            if (clientInfo == null) {
                clientInfo = new HashMap<>();
            } else {
                clientInfo.clear();
            }
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                setClientInfo((String) entry.getKey(),
                        (String) entry.getValue());
            }
        } catch (Exception e) {
            throw convertToClientInfoException(logAndConvert(e));
        }
    }

    /**
     * Get the client properties.
     *
     * @return the property list
     */
    @Override
    public Properties getClientInfo() throws SQLException {
        try {
            List<String> serverList = client.getNodes().stream().map(n -> n.getHost().toURI()).collect(Collectors.toList());
            Properties p = new Properties();

            if (clientInfo != null) {
                for (Map.Entry<String, ClientInfoStatus> entry : clientInfo.entrySet()) {
                    p.setProperty(entry.getKey(), entry.getValue().name());
                }
            }

            p.setProperty(NUM_SERVERS, Integer.toString(serverList.size()));
            for (int i = 0; i < serverList.size(); i++) {
                p.setProperty(PREFIX_SERVER + i, serverList.get(i));
            }

            return p;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get a client property.
     *
     * @param name the client info name
     * @return the property value or null if the property is not found or not
     * supported.
     */
    @Override
    public String getClientInfo(String name) throws SQLException {
        try {
            if (name == null) {
                throw new SQLException("name");
            }
            return getClientInfo().getProperty(name);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the given schema name to access.
     *
     * @param schema the schema name
     */
    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @return {@literal null}
     * @throws SQLException if the connection is closed
     */
    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @param executor The {@code Executor}  implementation which will
     *                 be used by {@code abort}.
     * @throws SQLException if the connection is closed
     */
    @Override
    public void abort(Executor executor) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     *
     * @param executor     The {@code Executor}  implementation which will
     *                     be used by {@code setNetworkTimeout}.
     * @param milliseconds The time in milliseconds to wait for the database
     *                     operation
     *                     to complete.  If the JDBC driver does not support milliseconds, the
     *                     JDBC driver will round the value up to the nearest second.  If the
     *                     timeout period expires before the operation
     *                     completes, a SQLException will be thrown.
     *                     A value of 0 indicates that there is not timeout for database operations.
     * @throws SQLException if the connection is closed
     */
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * <p><b>Unsupported Operation.</b></p>
     * <p>
     * Retrieves the number of milliseconds the driver will
     * wait for a database request to complete.
     * If the limit is exceeded, a
     * {@code SQLException} is thrown.
     *
     * @return the current timeout limit in milliseconds; zero means there is
     * no limit
     * @throws SQLException if the connection is closed
     */
    @Override
    public int getNetworkTimeout() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    /**
     * Log an exception and convert it to a SQL exception if required.
     *
     * @param ex the exception
     * @return the SQL exception object
     */
    protected SQLException logAndConvert(Throwable ex) {
        SQLException e = new SQLException("", ex);
        logger.severe(e.getMessage());
        return e;
    }
}
