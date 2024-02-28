package gr.uoa.di.madgik.elastic.sql;

public class ColumnInfo {

    String name;
    String type;

    public ColumnInfo() {
    }

    public ColumnInfo(String name, String type) {
    }

    public String getName() {
        return name;
    }

    public ColumnInfo setName(String name) {
        this.name = name;
        return this;
    }

    public String getType() {
        return type;
    }

    public ColumnInfo setType(String type) {
        this.type = type;
        return this;
    }
}
