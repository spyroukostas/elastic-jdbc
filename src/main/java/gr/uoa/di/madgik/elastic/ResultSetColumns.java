package gr.uoa.di.madgik.elastic;

import gr.uoa.di.madgik.elastic.sql.ColumnInfo;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ResultSetColumns {

    private final List<ColumnInfo> rsColumns;
    private List<String> rsColumnNames;
    private int size = 0;

    public ResultSetColumns() {
        this.rsColumns = new ArrayList<>();
        this.rsColumnNames = new ArrayList<>();
    }

    public ResultSetColumns(ColumnInfo[] rsColumns) {
        this.size = rsColumns.length;
        this.rsColumns = Arrays.asList(rsColumns);
        this.rsColumnNames = Arrays.stream(rsColumns).map(ColumnInfo::getName).collect(Collectors.toList());
    }

    public ResultSetColumns(List<ColumnInfo> rsColumns) {
        this.rsColumns = rsColumns;
    }

    public List<ColumnInfo> getRsColumns() {
        return rsColumns;
    }

    public ColumnInfo get(int index) throws SQLException {
        if (index < 1 || index > rsColumns.size()) {
            throw new SQLException("Index out of bounds");
        }
        return rsColumns.get(index - 1);

    }

    public List<String> getRsColumnNames() {
        return rsColumnNames;
    }

    public int getSize() {
        return size;
    }
}
