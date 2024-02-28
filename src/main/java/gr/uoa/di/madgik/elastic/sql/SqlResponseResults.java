package gr.uoa.di.madgik.elastic.sql;

import java.util.List;

public class SqlResponseResults {

    List<ColumnInfo> columns;
    List<List<Object>> rows;

    public SqlResponseResults() {
    }

    public List<ColumnInfo> getColumns() {
        return columns;
    }

    public SqlResponseResults setColumns(List<ColumnInfo> columns) {
        this.columns = columns;
        return this;
    }

    public List<List<Object>> getRows() {
        return rows;
    }

    public SqlResponseResults setRows(List<List<Object>> rows) {
        this.rows = rows;
        return this;
    }
}
