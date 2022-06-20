package com.beyond.tool;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author beyond
 * @date 2021/01/25
 */
public class SelectParser {

    public static List<SelectProperty> parse(String sql, Map<String, List<String>> focusColumns) throws IOException {

        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);

        TableCollectVisitor visitor = new TableCollectVisitor();
        sqlStatement.accept(visitor);

        List<SQLExprTableSource> sqlExprTableSource = visitor.getTableSources();
        List<TableName> list = new ArrayList<>();
        for (SQLExprTableSource exprTableSource : sqlExprTableSource) {
            TableName tableName = new TableName();
            tableName.setTableSource(exprTableSource);
            list.add(tableName);
        }
        for (TableName tableName : list) {
            complement(tableName);
        }

        // 域对应的from中的表, 不包含临时表
        Map<SQLSelectQueryBlock, List<TableName>> region2Tables = list.stream().collect(Collectors.groupingBy(TableName::getRegion));

        // 获取临时表的别名
        Map<TableAliasKey, SQLSelectQueryBlock> alias2QueryBlock = new HashMap<>();
        List<SQLSelectQueryBlock> selectQueryBlocks = visitor.getSelectQueryBlocks();
        for (SQLSelectQueryBlock selectQueryBlock : selectQueryBlocks) {
            SQLSelectQueryBlock region = findFirstParent(selectQueryBlock, SQLSelectQueryBlock.class);
            SQLTableSource sqlTableSource = findFirstParent(selectQueryBlock, SQLTableSource.class);
            if (sqlTableSource != null) {
                String alias = sqlTableSource.getAlias();
                alias2QueryBlock.put(new TableAliasKey(alias, region), selectQueryBlock);
            }
        }

        // 没有别名的临时表
        List<SQLSelectQueryBlock> nonAliasQueryBlocks = new ArrayList<>();
        // 在select中的子查询放入 nonAliasQueryBlocks
        for (SQLSelectQueryBlock selectQueryBlock : selectQueryBlocks) {
            if (findFirstParent(selectQueryBlock, SQLSelectItem.class) != null){
                nonAliasQueryBlocks.add(selectQueryBlock);
            }
        }


        List<Property> properties = new ArrayList<>();
        List<SQLSelectItem> rootSelectItems = ((SQLSelectStatement) sqlStatement).getSelect().getQueryBlock().getSelectList();
        for (SQLSelectItem selectItem : rootSelectItems) {
            Property property = new Property();
            SQLExpr expr = selectItem.getExpr();
            if (expr instanceof SQLPropertyExpr){
                property.setNameInRootSelect(((SQLPropertyExpr) expr).getName());
                property.setNameInRegionSelect(((SQLPropertyExpr) expr).getName());
                property.setOwner(((SQLPropertyExpr) expr).getOwnerName());
            }else{
                property.setNameInRootSelect(expr.toString());
                property.setNameInRegionSelect(expr.toString());
                property.setOwner(null);
            }
            property.setAliasInRootSelect(selectItem.getAlias());
            property.setItem(selectItem);
            properties.add(property);
        }

        properties.forEach(x -> {
            x.setNameInRegionSelect(org.apache.commons.lang3.StringUtils.strip(x.getNameInRegionSelect(), "`").toLowerCase());
            x.setNameInRootSelect(org.apache.commons.lang3.StringUtils.strip(x.getNameInRootSelect(), "`").toLowerCase());
        });

        List<SelectProperty> selectProperties = new ArrayList<>();
        for (Property property : properties) {
            TableName found = null;
            String alias = property.getOwner();
            String name = property.getNameInRootSelect();
            SQLSelectQueryBlock region = findFirstParent(property.getItem(), SQLSelectQueryBlock.class);
            List<TableName> tablesInRegion = region2Tables.get(region);
            if (alias != null) {
                found = findByAliasR(alias, tablesInRegion, region2Tables);
                if (found == null) {
                    // it is impossible, except a.order_id = 2343
                    found = findByAliasRR(region, alias, name, property, tablesInRegion, region2Tables, alias2QueryBlock,nonAliasQueryBlocks);
                    if (found == null) {
                        // it is impossible
                        throw new RuntimeException();
                    }
                }
            } else {
                //检查表结构中的字段名,确认是哪个表的字段
                for (TableName tableName : tablesInRegion) {
                    List<String> columns = focusColumns.get(tableName.getTableName());
                    if (columns != null && columns.contains(name)) {
                        found = tableName;
                        break;
                    }
                }
                if (found == null) {
                    // not focused, skip
                }
            }

            if (found != null) {
                SelectProperty selectProperty = new SelectProperty();
                selectProperty.setTableName(found.getTableName());
                selectProperty.setNameInRootSelect(property.getNameInRootSelect());
                selectProperty.setNameInRegionSelect(property.getNameInRegionSelect());
                selectProperty.setAliasInRootSelect(property.getAliasInRootSelect());
                selectProperties.add(selectProperty);
            }
        }

        return selectProperties;
    }

    /**
     * 根据别名和要查询的字段名递归查询, 为了处理临时表的情况
     *
     * @param alias            条件中表的别名
     * @param propName         条件中的字段名
     * @param list             当前域下的表信息 (不包括临时表)
     * @param region2Tables    域对应的表  (不包括临时表)
     * @param alias2QueryBlock 别名->别名所在域
     * @param nonAlias2QueryBlock
     * @return 表信息
     */
    private static TableName findByAliasRR(SQLSelectQueryBlock region, String alias, String propName, Property property, List<TableName> list, Map<SQLSelectQueryBlock, List<TableName>> region2Tables,
                                           Map<TableAliasKey, SQLSelectQueryBlock> alias2QueryBlock, List<SQLSelectQueryBlock> nonAliasQueryBlocks) {
        TableName tableName = findByAliasR(alias, list, region2Tables);
        if (tableName == null) {
            if (alias == null && region.getFrom() instanceof SQLSubqueryTableSource){
                // 处理只有一个虚拟表, select中没有指定虚拟表别名的
                alias = region.getFrom().getAlias();
            }
            if (propName == null && region.getSelectList().size() == 1){
                // 处理select中有子查询, 且只select一个字段
                SQLSelectItem theOnlySelectItem = region.getSelectItem(0);
                if (theOnlySelectItem.getExpr() instanceof SQLIdentifierExpr){
                    propName = theOnlySelectItem.toString();
                }
                if (theOnlySelectItem.getExpr() instanceof SQLPropertyExpr){
                    propName = ((SQLPropertyExpr) theOnlySelectItem.getExpr()).getName();
                }
            }
            SQLSelectQueryBlock foundVirtualTable = alias2QueryBlock.get(new TableAliasKey(alias, region));
            if (foundVirtualTable == null) {
                foundVirtualTable = nonAliasQueryBlocks.stream().filter(x -> x == region).findFirst().orElse(null);
                if (foundVirtualTable == null) {
                    return null;
                }
            }
            SQLSelectItem selectItem = foundVirtualTable.findSelectItem(propName);
            if (selectItem.getExpr() instanceof SQLPropertyExpr) {
                String nextAlias = ((SQLPropertyExpr) selectItem.getExpr()).getOwnerName();
                String nextPropName = ((SQLPropertyExpr) selectItem.getExpr()).getName();
                return findByAliasRR(foundVirtualTable,nextAlias, nextPropName, property, region2Tables.get(foundVirtualTable), region2Tables, alias2QueryBlock, nonAliasQueryBlocks);
            }
            if (selectItem.getExpr() instanceof SQLIdentifierExpr) {
                String nextPropName = ((SQLIdentifierExpr) selectItem.getExpr()).getName();
                return findByAliasRR(foundVirtualTable,null, nextPropName, property, region2Tables.get(foundVirtualTable), region2Tables, alias2QueryBlock, nonAliasQueryBlocks);
            }
            if (selectItem.getExpr() instanceof SQLQueryExpr) {
                //select中有子查询
                return findByAliasRR(((SQLQueryExpr) selectItem.getExpr()).getSubQuery().getQueryBlock(),null, null, property, region2Tables.get(foundVirtualTable), region2Tables, alias2QueryBlock, nonAliasQueryBlocks);
            }
        }else{
            property.setNameInRegionSelect(propName);
        }
        return tableName;
    }

    /**
     * 用别名向上递归查询, 即别名在本域内找不到表信息,则到parent域中寻找
     *
     * @param alias         别名
     * @param list          当前域下的表信息 (不包括临时表)
     * @param region2Tables 域对应的表  (不包括临时表)
     * @return 表信息
     */
    private static TableName findByAliasR(String alias, List<TableName> list, Map<SQLSelectQueryBlock, List<TableName>> region2Tables) {
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }
        TableName tableName = findByAlias(alias, list);
        if (tableName == null) {
            SQLSelectQueryBlock parentQueryBlock = findFirstParent(list.get(0).getRegion(), SQLSelectQueryBlock.class);
            if (parentQueryBlock == null) {
                return null;
            }
            return findByAliasR(alias, region2Tables.get(parentQueryBlock), region2Tables);
        }
        return tableName;
    }

    /**
     * 根据别名查询表信息
     *
     * @param alias 别名
     * @param list  某一个域下的表信息 (不包括临时表)
     */
    private static TableName findByAlias(String alias, List<TableName> list) {
        for (TableName tableName : list) {
            if (StringUtils.equals(tableName.getAlias(), alias)) {
                return tableName;
            }
            // 可能为别名写表名的情况, 如: ts_wholesale_order.id = 123.
            String[] split = StringUtils.split(tableName.getTableName(), "\\.");
            if (split.length == 1) {
                String simpleTableName = split[0];
                if (StringUtils.equals(simpleTableName, alias)) {
                    return tableName;
                }
            }
            if (split.length == 2) {
                String simpleTableName = split[1];
                if (StringUtils.equals(simpleTableName, alias)) {
                    return tableName;
                }
            }

        }
        return null;
    }

    private static class Property {
        /**
         * tablename the property belongs to
         */
        private String owner;
        private String nameInRootSelect;
        /**
         * 如有子查询代表最底部region中对应的字段名. 如无子查询则 = nameInRootSelect
         */
        private String nameInRegionSelect;
        private String aliasInRootSelect;
        private SQLObject item;

        public String getNameInRegionSelect() {
            return nameInRegionSelect;
        }

        public void setNameInRegionSelect(String nameInRegionSelect) {
            this.nameInRegionSelect = nameInRegionSelect;
        }

        public String getAliasInRootSelect() {
            return aliasInRootSelect;
        }

        public void setAliasInRootSelect(String aliasInRootSelect) {
            this.aliasInRootSelect = aliasInRootSelect;
        }

        public SQLObject getItem() {
            return item;
        }

        public void setItem(SQLObject item) {
            this.item = item;
        }

        public String getOwner() {
            return owner;
        }

        public void setOwner(String owner) {
            this.owner = owner;
        }

        public String getNameInRootSelect() {
            return nameInRootSelect;
        }

        public void setNameInRootSelect(String nameInRootSelect) {
            this.nameInRootSelect = nameInRootSelect;
        }
    }

    public static class SelectProperty{
        private String tableName;
        private String nameInRootSelect;
        private String nameInRegionSelect;

        private String aliasInRootSelect;

        public String getNameInRegionSelect() {
            return nameInRegionSelect;
        }

        public void setNameInRegionSelect(String nameInRegionSelect) {
            this.nameInRegionSelect = nameInRegionSelect;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getNameInRootSelect() {
            return nameInRootSelect;
        }

        public void setNameInRootSelect(String nameInRootSelect) {
            this.nameInRootSelect = nameInRootSelect;
        }

        public String getAliasInRootSelect() {
            return aliasInRootSelect;
        }
        public void setAliasInRootSelect(String aliasInRootSelect) {
            this.aliasInRootSelect = aliasInRootSelect;
        }


        @Override
        public String toString() {
            return "SelectProperty{" +
                    "tableName='" + tableName + '\'' +
                    ", nameInRootSelect='" + nameInRootSelect + '\'' +
                    ", nameInRegionSelect='" + nameInRegionSelect + '\'' +
                    ", aliasInRootSelect='" + aliasInRootSelect + '\'' +
                    '}';
        }
    }

    private static class TableName {
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

    private static class TableAliasKey {
        private final String alias;

        private final SQLSelectQueryBlock region;

        public TableAliasKey(String alias, SQLSelectQueryBlock region) {
            this.alias = alias;
            this.region = region;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TableAliasKey that = (TableAliasKey) o;

            if (!Objects.equals(alias, that.alias)) return false;
            return Objects.equals(region, that.region);
        }
        @Override
        public int hashCode() {
            int result = alias != null ? alias.hashCode() : 0;
            result = 31 * result + (region != null ? region.hashCode() : 0);
            return result;
        }

    }

    private static class TableCollectVisitor extends MySqlASTVisitorAdapter {

        private final List<SQLExprTableSource> tableSources = new ArrayList<>();

        private final List<SQLBinaryOpExpr> binaryOpExprs = new ArrayList<>();

        private final List<SQLBetweenExpr> sqlBetweenExprs = new ArrayList<>();

        private final List<SQLInListExpr> inListCondition = new ArrayList<>();

        private final List<SQLSelectQueryBlock> selectQueryBlocks = new ArrayList<>();


        @Override
        public boolean visit(SQLExprTableSource x) {
            tableSources.add(x);
            return true;
        }

        @Override
        public boolean visit(SQLBinaryOpExpr x) {
            binaryOpExprs.add(x);
            return true;
        }

        @Override
        public boolean visit(SQLInListExpr x) {
            inListCondition.add(x);
            return true;
        }

        @Override
        public boolean visit(SQLSelectQueryBlock x) {
            selectQueryBlocks.add(x);
            return true;
        }

        @Override
        public boolean visit(SQLBetweenExpr x) {
            sqlBetweenExprs.add(x);
            return true;
        }

        public List<SQLSelectQueryBlock> getSelectQueryBlocks() {
            return selectQueryBlocks;
        }

        public List<SQLExprTableSource> getTableSources() {
            return tableSources;
        }

        public List<SQLBinaryOpExpr> getBinaryOpExprs() {
            return binaryOpExprs;
        }

        public List<SQLInListExpr> getInListCondition() {
            return inListCondition;
        }

        public List<SQLBetweenExpr> getSqlBetweenExprs() {
            return sqlBetweenExprs;
        }
    }

    private static void complement(TableName tableName) {
        tableName.setRegion(findFirstParentQueryBlock(tableName.getTableSource()));
        tableName.setTableName(tableName.getTableSource().getExpr().toString());
        tableName.setAlias(tableName.getTableSource().getAlias2());
    }

    private static SQLSelectQueryBlock findFirstParentQueryBlock(SQLExprTableSource sqlExprTableSource) {
        SQLObject currParent = sqlExprTableSource.getParent();
        while (currParent != null) {
            if (currParent instanceof SQLSelectQueryBlock) {
                return (SQLSelectQueryBlock) currParent;
            }
            currParent = currParent.getParent();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <T extends SQLObject> T findFirstParent(SQLObject sqlObject, Class<T> tClass) {
        SQLObject currParent = sqlObject.getParent();
        while (currParent != null) {
            if (tClass.isAssignableFrom(currParent.getClass())) {
                return (T) currParent;
            }
            currParent = currParent.getParent();
        }
        return null;
    }
}
