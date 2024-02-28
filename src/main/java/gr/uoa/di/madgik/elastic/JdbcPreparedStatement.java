package gr.uoa.di.madgik.elastic;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {

    private final Logger logger = Logger.getLogger(JdbcPreparedStatement.class.getName());

    protected final String sql;
    private final Map<Integer, String> parameters = new HashMap<>();

    public JdbcPreparedStatement(JdbcConnection con, String sql) {
        super(con);
        this.sql = sql;
    }

    public JdbcPreparedStatement(JdbcConnection con, String sql, int rsType, int rsConcurrency, int rsHoldability) throws SQLException {
        super(con, rsType, rsConcurrency, rsHoldability);
        this.sql = sql;
    }


    @Override
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        // TODO: refactor
        String translated = sql;
        for (int i = 1; i <= parameters.size(); i++) {
            if (parameters.get(i).equals("'null'")) {
                String part = translated.split("\\?", 2)[0];
                if (part.endsWith("!=")) {
                    translated = translated.replaceFirst("!=\\?", " is NOT NULL ");
                } else if (part.endsWith("=")) {
                    translated = translated.replaceFirst("=\\?", " is NULL ");
                } else {
                    translated = translated.replaceFirst("\\?", parameters.get(i));
                }
            } else
                translated = translated.replaceFirst("\\?", parameters.get(i));
        }
        // FIXME: move to nativeSQL?
        String[] parts = translated.split("GROUP BY", 2);
        if (parts[1].matches("\\s+[a-z0-9]+\\.[a-z0-9]+\\..*")) {
            translated = translated.split("GROUP BY", 2)[0];
        }
        return executeQuery(translated);
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkClosed();
        this.parameters.put(parameterIndex, "null");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkClosed();
        this.parameters.put(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkClosed();
        this.parameters.put(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkClosed();
        this.parameters.put(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkClosed();
        this.parameters.put(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkClosed();
        this.parameters.put(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkClosed();
        this.parameters.put(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkClosed();
        this.parameters.put(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkClosed();
        this.parameters.put(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkClosed();
        this.parameters.put(parameterIndex, String.format("'%s'", x));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkClosed();
        this.parameters.put(parameterIndex, Arrays.toString(x));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        checkClosed();
        this.parameters.put(parameterIndex, String.valueOf(x.toInstant()));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkClosed();
        this.parameters.put(parameterIndex, String.valueOf(x.toInstant()));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        checkClosed();
        this.parameters.put(parameterIndex, String.valueOf(x.toInstant()));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkClosed();

    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkClosed();

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkClosed();

    }

    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        this.parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        checkClosed();
        this.parameters.put(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkClosed();
        String item = String.valueOf(x);
        if (!item.matches("[0-9]+\\.?[0-9]*")) {
            item = "'" + item + "'";
        }
        this.parameters.put(parameterIndex, item);
    }

    @Override
    public boolean execute() throws SQLException {
        executeQuery();
        return getResultSet() != null;
    }

    @Override
    public void addBatch() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return this.resultSet.getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

}
