package com.beyond.tool;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;

import java.util.ArrayList;
import java.util.List;

public class RoutingSource {
    private TableName tableName;
    private String table;
    private SQLSelectQueryBlock region;
    private String propName;

    private List<Integer> values = new ArrayList<>();

    private List<Range> ranges = new ArrayList<>();

    private SQLObject condition;

    public TableName getTableName() {
        return tableName;
    }

    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getPropName() {
        return propName;
    }

    public void setPropName(String propName) {
        this.propName = propName;
    }

    public List<Integer> getValues() {
        return values;
    }

    public void setValues(List<Integer> values) {
        this.values = values;
    }

    public List<Range> getRanges() {
        return ranges;
    }

    public void setRanges(List<Range> ranges) {
        this.ranges = ranges;
    }

    public SQLSelectQueryBlock getRegion() {
        return region;
    }

    public void setRegion(SQLSelectQueryBlock region) {
        this.region = region;
    }

    public SQLObject getCondition() {
        return condition;
    }

    public void setCondition(SQLObject condition) {
        this.condition = condition;
    }


    @Override
    public String toString() {
        StringBuilder s = new StringBuilder(table + "." + propName + " in " + values);
        for (Range range : ranges) {
            s.append(", range: ");
            if (range.isIncludeLow()){
                s.append("[");
            }else{
                s.append("(");
            }
            s.append(range.getLow());
            s.append(",");
            s.append(range.getHigh());
            if (range.isIncludeHigh()){
                s.append("]");
            }else{
                s.append(")");
            }
        }
        return s.toString();
    }
}