package com.beyond.tool;

import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;

import java.util.ArrayList;
import java.util.List;

public class MergedRoutingSource {
    private String table;
    private SQLSelectQueryBlock region;
    private String propName;

    /**
     * 与ranges为交集
     * 如果values = 3 ranges=[1-6], 那应该取3
     */
    private List<Integer> values = new ArrayList<>();

    /**
     * 与values为交集
     */
    private List<Range> ranges = new ArrayList<>();

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public SQLSelectQueryBlock getRegion() {
        return region;
    }

    public void setRegion(SQLSelectQueryBlock region) {
        this.region = region;
    }

    public String getPropName() {
        return propName;
    }

    public void setPropName(String propName) {
        this.propName = propName;
    }

    public List<Range> getRanges() {
        return ranges;
    }

    public void setRanges(List<Range> ranges) {
        this.ranges = ranges;
    }

    public List<Integer> getValues() {
        return values;
    }

    public void setValues(List<Integer> values) {
        this.values = values;
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