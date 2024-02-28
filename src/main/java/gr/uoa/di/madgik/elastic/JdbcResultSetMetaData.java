package gr.uoa.di.madgik.elastic;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.logging.Logger;

public class JdbcResultSetMetaData extends JdbcWrapper implements ResultSetMetaData {

    private static final Logger logger = Logger.getLogger(JdbcResultSetMetaData.class.getName());

    private ResultSetColumns rsColumns;

    public JdbcResultSetMetaData() {
    }

    public JdbcResultSetMetaData(ResultSetColumns columns) {
        this.rsColumns = columns;
    }

    private void checkRsColumns() throws SQLException {
        if (rsColumns == null || rsColumns.getRsColumns() == null) {
            throw new SQLException("Columns not initialized");
        }
    }

    @Override
    public int getColumnCount() throws SQLException {
        checkRsColumns();
        return rsColumns.getSize();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        checkRsColumns();
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        checkRsColumns();
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        checkRsColumns();
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        checkRsColumns();
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        checkRsColumns();
        return columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        checkRsColumns();
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        checkRsColumns();
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        checkRsColumns();
        return this.rsColumns.get(column).getType();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        checkRsColumns();
        return this.rsColumns.get(column).getType();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        checkRsColumns();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        checkRsColumns();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public int getScale(int column) throws SQLException {
        checkRsColumns();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public String getTableName(int column) throws SQLException {
        checkRsColumns();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        checkRsColumns();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        checkRsColumns();
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        checkRsColumns();
        return this.rsColumns.get(column).getType();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        checkRsColumns();
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        checkRsColumns();
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        checkRsColumns();
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        checkRsColumns();
        throw new UnsupportedOperationException("Unsupported Operation");
    }
}
