package com.beyond.tool;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenshipeng
 * @date 2021/11/04
 */
public class TableName {
    private SQLSelectQueryBlock region;
    private SQLExprTableSource tableSource;
    private String tableName;
    private String alias;
    private List<String> columnNames = new ArrayList<>();

    public SQLExprTableSource getTableSource() {
        return tableSource;
    }

    public void setTableSource(SQLExprTableSource tableSource) {
        this.tableSource = tableSource;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public SQLSelectQueryBlock getRegion() {
        return region;
    }

    public void setRegion(SQLSelectQueryBlock region) {
        this.region = region;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }
}
