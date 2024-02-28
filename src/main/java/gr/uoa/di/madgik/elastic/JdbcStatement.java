package gr.uoa.di.madgik.elastic;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.sql.*;
import java.util.logging.Logger;

public class JdbcStatement extends JdbcWrapper implements Statement {

    private static final Logger logger = Logger.getLogger(JdbcStatement.class.getName());

    protected Connection connection;
    protected RestClient client;
    protected ResultSet resultSet;
    protected long maxRows = 0;

    private volatile boolean closed;
    private boolean closeOnCompletion;

    public JdbcStatement(JdbcConnection connection) {
        this.connection = connection;
        this.client = connection.getClient();
    }

    public JdbcStatement(JdbcConnection connection, int rsType, int rsConcurrency, int rsHoldability) throws SQLException {
        this.connection = connection;
        this.client = connection.getClient();

        if (rsType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("rsType supports only ResultSet.TYPE_FORWARD_ONLY");
        }
        if (rsConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("rsConcurrency supports only ResultSet.CONCUR_READ_ONLY");
        }
        if (rsHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLFeatureNotSupportedException("rsHoldability supports only ResultSet.CLOSE_CURSORS_AT_COMMIT");
        }
    }

    protected void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("This statement has been closed.");
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (!execute(sql)) {
            throw new SQLException("Error executing sql query: " + sql);
        }
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
            resultSet.close();
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkClosed();
        return Math.toIntExact(maxRows);
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        checkClosed();
        this.maxRows = max;
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        checkClosed();
        return maxRows;
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        checkClosed();
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void cancel() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        // TODO: refactor
        try {
            Request request = new Request("GET", "/_sql");
            request.addParameter("format", "json");
            String requestBody = String.format("{\"query\": \"%s\"}", sql);
            request.setJsonEntity(requestBody);

            Response response = this.client.performRequest(request);
            this.resultSet = new JdbcResultSet(this, response);
            return true;
        } catch (Exception e) {
            logger.severe(e.getMessage());
        }
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return this.resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return this.connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkClosed();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkClosed();
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkClosed();
        return closeOnCompletion;
    }
}
