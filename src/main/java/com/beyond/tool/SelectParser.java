package com.beyond.tool;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * @author beyond
 * @date 2021/01/25
 */
public class SelectParser {

    /**
     * 添加加密字段
     */
    public static String parseInsert2(String sql, Map<String, Map<String, FocusParam>> focusColumns) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);

        TableCollectVisitor visitor = new TableCollectVisitor();
        sqlStatement.accept(visitor);

        if (sqlStatement instanceof SQLInsertStatement){
            String tablename = ((SQLInsertStatement) sqlStatement).getTableSource().getExpr().toString();
            Map<String, FocusParam> column2FocusParam = focusColumns.get(tablename);
            if (column2FocusParam == null){
                return sql;
            }
            List<SQLExpr> columns = ((SQLInsertStatement) sqlStatement).getColumns();
            Map<Integer, FocusParam> toEncrypt = new HashMap<>();
            int i = 0;
            for (SQLExpr column : columns) {
                if (column instanceof SQLIdentifierExpr){
                    String name = ((SQLIdentifierExpr) column).getName();
                    FocusParam focusParam = column2FocusParam.get(name);
                    if (focusParam != null){
                        toEncrypt.put(i, focusParam);
                    }
                }
                i++;
            }
            Map<Integer, Integer> encrypted2ToEncrypt = new HashMap<>();
            for (Integer index : toEncrypt.keySet()) {
                FocusParam toEncryptItem = toEncrypt.get(index);
                SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr();
                sqlIdentifierExpr.setParent(columns.get(index));
                sqlIdentifierExpr.setName(toEncryptItem.getsName());
                ((SQLInsertStatement) sqlStatement).addColumn(sqlIdentifierExpr);
                encrypted2ToEncrypt.put(((SQLInsertStatement) sqlStatement).getColumns().size()-1, index);
            }
            List<SQLInsertStatement.ValuesClause> valuesClauses = ((SQLInsertStatement) sqlStatement).getValuesList();
            for (SQLInsertStatement.ValuesClause valuesClause : valuesClauses) {
                for (Integer index : encrypted2ToEncrypt.keySet()) {
                    int encryptIndex = encrypted2ToEncrypt.get(index);
                    FocusParam focusParam = toEncrypt.get(encryptIndex);
                    SQLExpr sourceValue = valuesClause.getValues().get(encryptIndex);
                    if (sourceValue instanceof SQLTextLiteralExpr) {
                        String origin = ((SQLTextLiteralExpr) sourceValue).getText();
                        SQLCharExpr sqlCharExpr = new SQLCharExpr();
                        sqlCharExpr.setParent(sourceValue.getParent());
                        sqlCharExpr.setText(focusParam.getM().apply(origin, focusParam.getKey()));
                        if (sourceValue.getParent() instanceof SQLInsertStatement.ValuesClause) {
                            ((SQLInsertStatement.ValuesClause) sourceValue.getParent()).addValue(sqlCharExpr);
                        }
                    }
                    if (sourceValue instanceof SQLIntegerExpr) {
                        Number number = ((SQLIntegerExpr) sourceValue).getNumber();
                        String origin = number.toString();
                        SQLCharExpr sqlCharExpr = new SQLCharExpr();
                        sqlCharExpr.setParent(sourceValue.getParent());
                        sqlCharExpr.setText(focusParam.getM().apply(origin, focusParam.getKey()));
                        if (sourceValue.getParent() instanceof SQLInsertStatement.ValuesClause) {
                            ((SQLInsertStatement.ValuesClause) sourceValue.getParent()).addValue(sqlCharExpr);
                        }
                    }
                }
            }
        }

        return sqlStatement.toString();
    }

    /**
     * 替换加密字段
     */
    public static String parseInsert(String sql, Map<String, Map<String, FocusParam>> focusColumns) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);

        TableCollectVisitor visitor = new TableCollectVisitor();
        sqlStatement.accept(visitor);

        if (sqlStatement instanceof SQLInsertStatement){
            String tablename = ((SQLInsertStatement) sqlStatement).getTableSource().getExpr().toString();
            Map<String, FocusParam> column2FocusParam = focusColumns.get(tablename);
            if (column2FocusParam == null){
                return sql;
            }
            List<SQLExpr> columns = ((SQLInsertStatement) sqlStatement).getColumns();
            Map<Integer, FocusParam> toEncrypt = new HashMap<>();
            int i = 0;
            for (SQLExpr column : columns) {
                if (column instanceof SQLIdentifierExpr){
                    String name = ((SQLIdentifierExpr) column).getName();
                    FocusParam focusParam = column2FocusParam.get(name);
                    if (focusParam != null){
                        toEncrypt.put(i, focusParam);
                    }
                }
                i++;
            }
            List<SQLInsertStatement.ValuesClause> valuesClauses = ((SQLInsertStatement) sqlStatement).getValuesList();
            for (SQLInsertStatement.ValuesClause valuesClause : valuesClauses) {
                for (Integer index : toEncrypt.keySet()) {
                    FocusParam focusParam = toEncrypt.get(index);
                    SQLExpr sqlExpr = valuesClause.getValues().get(index);
                    safeReplace(sqlExpr, focusParam);
                }
            }
        }
        return sqlStatement.toString();
    }

    public static Result parse(String sql, Map<String, Map<String, FocusParam>> focusColumns) {

        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);

        TableCollectVisitor visitor = new TableCollectVisitor();
        sqlStatement.accept(visitor);

        // 这个方法可以解析property中的owner属于哪个表, 在resolvedOwnerObj, 如果是子查询, 会是一个子查询的obj
        SchemaStatVisitor schemaStatVisitor = SQLUtils.createSchemaStatVisitor(DbType.mysql);
        sqlStatement.accept(schemaStatVisitor);

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
            if (findFirstParent(selectQueryBlock, SQLSelectItem.class) != null) {
                nonAliasQueryBlocks.add(selectQueryBlock);
            }
        }


        List<Property> properties = new ArrayList<>();

        List<SQLSelectItem> rootSelectItems = ((SQLSelectStatement) sqlStatement).getSelect().getQueryBlock().getSelectList();
        List<SQLSelectItem> allSelectItems = visitor.getSelectItems();
        for (SQLSelectItem selectItem : allSelectItems) {
            SelectProperty property = new SelectProperty();
            SQLExpr expr = selectItem.getExpr();
            if (expr instanceof SQLPropertyExpr) {
                property.setName(((SQLPropertyExpr) expr).getName());
                property.setOwner(((SQLPropertyExpr) expr).getOwnerName());
            } else {
                property.setName(expr.toString());
                property.setOwner(null);
            }
            property.setAlias(selectItem.getAlias());
            property.setItem(selectItem);
            property.setBottomItem(selectItem);
            property.setRoot(rootSelectItems.contains(selectItem));
            properties.add(property);
        }

        for (SQLSelectItem selectItem : allSelectItems) {
            SelectProperty property = new SelectProperty();
            SQLExpr expr = selectItem.getExpr();
            SQLMethodInvokeExpr methodInvokeExpr = findFirstChild(expr, SQLMethodInvokeExpr.class);
            if (methodInvokeExpr != null) {
                List<SQLPropertyExpr> sqlProperties = new ArrayList<>();
                findChildren(methodInvokeExpr, SQLPropertyExpr.class, sqlProperties);
                if (CollectionUtils.isNotEmpty(sqlProperties)) {
                    for (SQLPropertyExpr sqlProperty : sqlProperties) {
                        property.setName(sqlProperty.getName());
                        property.setOwner(sqlProperty.getOwnerName());
                        property.setAlias(selectItem.getAlias());
                        property.setItem(selectItem);
                        property.setBottomItem(selectItem);
                        property.setRoot(rootSelectItems.contains(selectItem));
                        properties.add(property);
                    }
                } else {
                    List<SQLIdentifierExpr> sqlIdentifierExprs = new ArrayList<>();
                    findChildren(methodInvokeExpr, SQLIdentifierExpr.class, sqlIdentifierExprs);
                    for (SQLIdentifierExpr sqlIdentifierExpr : sqlIdentifierExprs) {
                        property.setName(sqlIdentifierExpr.getName());
                        property.setOwner(null);
                        property.setAlias(selectItem.getAlias());
                        property.setItem(selectItem);
                        property.setBottomItem(selectItem);
                        property.setRoot(rootSelectItems.contains(selectItem));
                        properties.add(property);
                    }
                }
            }
        }


        for (SQLBinaryOpExpr condition : visitor.getBinaryOpExprs()) {
            SQLExpr left = condition.getLeft();
            if (left instanceof SQLPropertyExpr) {
                BinaryOpConditionProperty conditionProperty = new BinaryOpConditionProperty();
                conditionProperty.setItem(condition);
                conditionProperty.setOwner(((SQLPropertyExpr) left).getOwnerName());
                conditionProperty.setName(((SQLPropertyExpr) left).getName());
                conditionProperty.setLeft(true);
                properties.add(conditionProperty);
            }

            if (left instanceof SQLIdentifierExpr) {
                BinaryOpConditionProperty conditionProperty = new BinaryOpConditionProperty();
                conditionProperty.setItem(condition);
                conditionProperty.setOwner(null);
                conditionProperty.setName(((SQLIdentifierExpr) left).getName());
                conditionProperty.setLeft(true);
                properties.add(conditionProperty);
            }
            SQLExpr right = condition.getRight();
            if (right instanceof SQLPropertyExpr) {
                BinaryOpConditionProperty conditionProperty = new BinaryOpConditionProperty();
                conditionProperty.setItem(condition);
                conditionProperty.setOwner(((SQLPropertyExpr) right).getOwnerName());
                conditionProperty.setName(((SQLPropertyExpr) right).getName());
                conditionProperty.setLeft(false);
                properties.add(conditionProperty);
            }

            if (right instanceof SQLIdentifierExpr) {
                BinaryOpConditionProperty conditionProperty = new BinaryOpConditionProperty();
                conditionProperty.setItem(condition);
                conditionProperty.setOwner(null);
                conditionProperty.setName(((SQLIdentifierExpr) right).getName());
                conditionProperty.setLeft(false);
                properties.add(conditionProperty);
            }
        }

        // 处理 in / not in
        for (SQLInListExpr inListExpr : visitor.getInListCondition()) {
            if (inListExpr.getExpr() instanceof SQLPropertyExpr) {
                if (inListExpr.isNot()) {
                    NotInProperty notInProperty = new NotInProperty();
                    notInProperty.setOwner(((SQLPropertyExpr) inListExpr.getExpr()).getOwnerName());
                    notInProperty.setName(((SQLPropertyExpr) inListExpr.getExpr()).getName());
                    notInProperty.setItem(inListExpr);
                    properties.add(notInProperty);
                } else {
                    InProperty inProperty = new InProperty();
                    inProperty.setOwner(((SQLPropertyExpr) inListExpr.getExpr()).getOwnerName());
                    inProperty.setName(((SQLPropertyExpr) inListExpr.getExpr()).getName());
                    inProperty.setItem(inListExpr);
                    properties.add(inProperty);
                }
            }

            if (inListExpr.getExpr() instanceof SQLIdentifierExpr) {
                if (inListExpr.isNot()) {
                    NotInProperty notInProperty = new NotInProperty();
                    notInProperty.setOwner(null);
                    notInProperty.setName(((SQLIdentifierExpr) inListExpr.getExpr()).getName());
                    notInProperty.setItem(inListExpr);
                    properties.add(notInProperty);
                } else {
                    InProperty inProperty = new InProperty();
                    inProperty.setOwner(null);
                    inProperty.setName(((SQLIdentifierExpr) inListExpr.getExpr()).getName());
                    inProperty.setItem(inListExpr);
                    properties.add(inProperty);
                }
            }
        }

        // 处理 in / not in subQuery
        for (SQLInSubQueryExpr inSubQueryExpr : visitor.getInSubQueryCondition()) {
            if (inSubQueryExpr.getExpr() instanceof SQLPropertyExpr) {
                if (inSubQueryExpr.isNot()) {
                    NotInSubQueryProperty notInProperty = new NotInSubQueryProperty();
                    notInProperty.setOwner(((SQLPropertyExpr) inSubQueryExpr.getExpr()).getOwnerName());
                    notInProperty.setName(((SQLPropertyExpr) inSubQueryExpr.getExpr()).getName());
                    notInProperty.setItem(inSubQueryExpr);
                    properties.add(notInProperty);
                } else {
                    InSubQueryProperty inProperty = new InSubQueryProperty();
                    inProperty.setOwner(((SQLPropertyExpr) inSubQueryExpr.getExpr()).getOwnerName());
                    inProperty.setName(((SQLPropertyExpr) inSubQueryExpr.getExpr()).getName());
                    inProperty.setItem(inSubQueryExpr);
                    properties.add(inProperty);
                }
            }

            if (inSubQueryExpr.getExpr() instanceof SQLIdentifierExpr) {
                if (inSubQueryExpr.isNot()) {
                    NotInSubQueryProperty notInProperty = new NotInSubQueryProperty();
                    notInProperty.setOwner(null);
                    notInProperty.setName(((SQLIdentifierExpr) inSubQueryExpr.getExpr()).getName());
                    notInProperty.setItem(inSubQueryExpr);
                    properties.add(notInProperty);
                } else {
                    InSubQueryProperty inProperty = new InSubQueryProperty();
                    inProperty.setOwner(null);
                    inProperty.setName(((SQLIdentifierExpr) inSubQueryExpr.getExpr()).getName());
                    inProperty.setItem(inSubQueryExpr);
                    properties.add(inProperty);
                }
            }
        }

        // 处理 between ?
        List<SQLBetweenExpr> betweenConditions = visitor.getSqlBetweenExprs();
        for (SQLBetweenExpr betweenCondition : betweenConditions) {
            SQLExpr testExpr = betweenCondition.getTestExpr();
            if (testExpr instanceof SQLPropertyExpr) {
                BetweenProperty betweenProperty = new BetweenProperty();
                betweenProperty.setOwner(((SQLPropertyExpr) testExpr).getOwnerName());
                betweenProperty.setName(((SQLPropertyExpr) testExpr).getName());
                betweenProperty.setItem(betweenCondition);
                properties.add(betweenProperty);
            }

            if (testExpr instanceof SQLIdentifierExpr) {
                BetweenProperty betweenProperty = new BetweenProperty();
                betweenProperty.setOwner(null);
                betweenProperty.setName(((SQLIdentifierExpr) testExpr).getName());
                betweenProperty.setItem(betweenCondition);
                properties.add(betweenProperty);
            }
        }

        properties.forEach(x -> {
            x.setName(org.apache.commons.lang3.StringUtils.strip(x.getName(), "`").toLowerCase());
            x.setColumn(x.getName());
            x.setBottomItem(x.getItem());
            if (x instanceof BinaryOpConditionProperty) {
                SQLBinaryOpExpr condition = (SQLBinaryOpExpr) x.getItem();
                if (((BinaryOpConditionProperty) x).isLeft()) {
                    x.setBottomItem(condition.getLeft());
                } else {
                    x.setBottomItem(condition.getRight());
                }
            }

            if (x instanceof BetweenProperty) {
                SQLBetweenExpr condition = (SQLBetweenExpr) x.getItem();
                x.setBottomItem(condition.getTestExpr());
            }

            if (x instanceof InProperty || x instanceof NotInProperty) {
                SQLInListExpr condition = (SQLInListExpr) x.getItem();
                x.setBottomItem(condition.getExpr());
            }
            if (x instanceof InSubQueryProperty || x instanceof NotInSubQueryProperty) {
                SQLInSubQueryExpr condition = (SQLInSubQueryExpr) x.getItem();
                x.setBottomItem(condition.getExpr());
            }
        });


        for (Property property : properties) {
            TableName found = null;
            String alias = property.getOwner();
            String name = property.getName();
            SQLSelectQueryBlock region = findFirstParent(property.getItem(), SQLSelectQueryBlock.class);
            List<TableName> tablesInRegion = region2Tables.get(region);
            if (alias != null) {
                found = findByAliasR(alias, tablesInRegion, region2Tables);
                if (found == null) {
                    // it is impossible, except a.order_id = 2343
                    found = findByAliasRR(region, alias, name, property, tablesInRegion, region2Tables, alias2QueryBlock, nonAliasQueryBlocks);
                    if (found == null) {
                        // it is impossible
                        throw new RuntimeException();
                    }
                }
            } else {
                //检查表结构中的字段名,确认是哪个表的字段
                for (TableName tableName : tablesInRegion) {
                    Map<String, FocusParam> replaceMapping = focusColumns.get(tableName.getTableName());
                    if (replaceMapping != null && replaceMapping.containsKey(name)) {
                        found = tableName;
                        break;
                    }
                }
                if (found == null) {
                    // not focused, skip
                }
            }

            if (found != null) {
                property.setTableName(found.getTableName());
            }
        }

        for (Property property : properties) {
            Map<String, FocusParam> replaceMapping = focusColumns.get(property.getTableName());
            if (replaceMapping == null || !replaceMapping.containsKey(property.getColumn())) {
                continue;
            }

            if (property.getBottomItem() instanceof SQLSelectItem) {
                SQLSelectItem regionSelect = (SQLSelectItem) property.getBottomItem();
                if (regionSelect.getAlias() == null) {
                    regionSelect.setAlias(property.getColumn());
                }

                FocusParam focusParam = replaceMapping.get(property.getColumn());
                String encryptedName = focusParam.getsName();
                if (regionSelect.getExpr() instanceof SQLIdentifierExpr) {
                    ((SQLIdentifierExpr) regionSelect.getExpr()).setName(encryptedName);
                }

                if (regionSelect.getExpr() instanceof SQLPropertyExpr) {
                    ((SQLPropertyExpr) regionSelect.getExpr()).setName(encryptedName);
                }
            }

            if (property.getBottomItem() instanceof SQLPropertyExpr) {
                FocusParam focusParam = replaceMapping.get(property.getColumn());
                String encryptedName = focusParam.getsName();
                SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) property.getBottomItem();
                if (sqlPropertyExpr != null) {
                    sqlPropertyExpr.setName(encryptedName);
                    encryptProperty(property, focusParam);
                }
            }

            if (property.getBottomItem() instanceof SQLIdentifierExpr) {
                FocusParam focusParam = replaceMapping.get(property.getColumn());
                String encryptedName = focusParam.getsName();
                SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) property.getBottomItem();
                if (sqlIdentifierExpr != null) {
                    sqlIdentifierExpr.setName(encryptedName);
                    encryptProperty(property, focusParam);
                }
            }
        }


        Result result = new Result();
        result.setSqlStatement(sqlStatement);
        result.setReplacedSql(sqlStatement.toString());
        result.setSelectProperties(properties.stream().filter(x -> x instanceof SelectProperty).map(x -> (SelectProperty) x).filter(SelectProperty::isRoot).collect(Collectors.toList()));

        return result;
    }


    private static void encryptProperty(Property property, FocusParam focusParam) {
        if (property instanceof BinaryOpConditionProperty) {
            SQLBinaryOpExpr item = (SQLBinaryOpExpr) property.getItem();
            safeReplace(item.getLeft(), focusParam);
            safeReplace(item.getRight(), focusParam);
        }

        if (property instanceof BetweenProperty) {
            SQLBetweenExpr item = (SQLBetweenExpr) property.getItem();
            safeReplace(item.getBeginExpr(), focusParam);
            safeReplace(item.getEndExpr(), focusParam);
        }

        if (property instanceof InProperty) {
            SQLInListExpr item = (SQLInListExpr) property.getItem();
            List<SQLExpr> targetList = item.getTargetList();
            for (SQLExpr sqlExpr : targetList) {
                safeReplace(sqlExpr, focusParam);
            }
        }
    }

    private static void safeReplace(SQLExpr sqlExpr, FocusParam focusParam) {
        if (sqlExpr instanceof SQLTextLiteralExpr) {
            String origin = ((SQLTextLiteralExpr) sqlExpr).getText();
            ((SQLTextLiteralExpr) sqlExpr).setText(focusParam.getM().apply(origin, focusParam.getKey()));
        }
        if (sqlExpr instanceof SQLIntegerExpr) {
            Number number = ((SQLIntegerExpr) sqlExpr).getNumber();
            String s = number.toString();
            SQLCharExpr sqlCharExpr = new SQLCharExpr();
            sqlCharExpr.setParent(sqlExpr.getParent());
            sqlCharExpr.setText(focusParam.getM().apply(s, focusParam.getKey()));

            if (sqlExpr.getParent() instanceof SQLBinaryOpExpr) {
                if (sqlExpr == ((SQLBinaryOpExpr) sqlExpr.getParent()).getLeft()) {
                    ((SQLBinaryOpExpr) sqlExpr.getParent()).setLeft(sqlCharExpr);
                }
                if (sqlExpr == ((SQLBinaryOpExpr) sqlExpr.getParent()).getRight()) {
                    ((SQLBinaryOpExpr) sqlExpr.getParent()).setRight(sqlCharExpr);
                }
            }

            if (sqlExpr.getParent() instanceof SQLInListExpr) {
                ((SQLInListExpr) sqlExpr.getParent()).replace(sqlExpr, sqlCharExpr);
            }

            if (sqlExpr.getParent() instanceof SQLBetweenExpr) {
                if (sqlExpr == ((SQLBetweenExpr) sqlExpr.getParent()).getBeginExpr()) {
                    ((SQLBetweenExpr) sqlExpr.getParent()).setBeginExpr(sqlCharExpr);
                }
                if (sqlExpr == ((SQLBetweenExpr) sqlExpr.getParent()).getEndExpr()) {
                    ((SQLBetweenExpr) sqlExpr.getParent()).setEndExpr(sqlCharExpr);
                }
            }
            if (sqlExpr.getParent() instanceof SQLInsertStatement.ValuesClause) {
                ((SQLInsertStatement.ValuesClause) sqlExpr.getParent()).replace(sqlExpr, sqlCharExpr);
            }
        }
    }


    /**
     * 根据别名和要查询的字段名递归查询, 为了处理临时表的情况
     *
     * @param alias               条件中表的别名
     * @param propName            条件中的字段名
     * @param list                当前域下的表信息 (不包括临时表)
     * @param region2Tables       域对应的表  (不包括临时表)
     * @param alias2QueryBlock    别名->别名所在域
     * @param nonAliasQueryBlocks 没有别名的子查询
     * @return 表信息
     */
    private static TableName findByAliasRR(SQLSelectQueryBlock region, String alias, String propName, Property property, List<TableName> list, Map<SQLSelectQueryBlock, List<TableName>> region2Tables,
                                           Map<TableAliasKey, SQLSelectQueryBlock> alias2QueryBlock, List<SQLSelectQueryBlock> nonAliasQueryBlocks) {
        TableName tableName = findByAliasR(alias, list, region2Tables);
        if (tableName == null) {
            if (alias == null && region.getFrom() instanceof SQLSubqueryTableSource) {
                // 处理只有一个虚拟表, select中没有指定虚拟表别名的
                alias = region.getFrom().getAlias();
            }
            if (propName == null && region.getSelectList().size() == 1) {
                // 处理select中有子查询, 且只select一个字段
                SQLSelectItem theOnlySelectItem = region.getSelectItem(0);
                if (theOnlySelectItem.getExpr() instanceof SQLIdentifierExpr) {
                    propName = theOnlySelectItem.toString();
                }
                if (theOnlySelectItem.getExpr() instanceof SQLPropertyExpr) {
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
                return findByAliasRR(foundVirtualTable, nextAlias, nextPropName, property, region2Tables.get(foundVirtualTable), region2Tables, alias2QueryBlock, nonAliasQueryBlocks);
            }
            if (selectItem.getExpr() instanceof SQLIdentifierExpr) {
                String nextPropName = ((SQLIdentifierExpr) selectItem.getExpr()).getName();
                return findByAliasRR(foundVirtualTable, null, nextPropName, property, region2Tables.get(foundVirtualTable), region2Tables, alias2QueryBlock, nonAliasQueryBlocks);
            }
            if (selectItem.getExpr() instanceof SQLQueryExpr) {
                //select中有子查询
                return findByAliasRR(((SQLQueryExpr) selectItem.getExpr()).getSubQuery().getQueryBlock(), null, null, property, region2Tables.get(foundVirtualTable), region2Tables, alias2QueryBlock, nonAliasQueryBlocks);
            }
        } else {
            property.setColumn(propName);
            property.setBottomItem(region.findSelectItem(propName));
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

    private static <T> void  findChildren(SQLExpr sqlExpr, Class<T> tClass, List<T> list) {
        if (tClass.isAssignableFrom(sqlExpr.getClass())){
            list.add((T) sqlExpr);
            return;
        }
        List<SQLObject> children = sqlExpr.getChildren();
        if (CollectionUtils.isEmpty(children)){
            return;
        }
        for (SQLObject child : children) {
            if (tClass.isAssignableFrom(child.getClass())){
                list.add((T) child);
            }
        }
        for (SQLObject child : children) {
            if (child instanceof SQLExpr){
                findChildren((SQLExpr) child, tClass, list);
            }
        }
    }

    private static  <T> T findFirstChild(SQLExpr sqlExpr, Class<T> tClass){
        if (tClass.isAssignableFrom(sqlExpr.getClass())){
            return (T) sqlExpr;
        }
        List<SQLObject> children = sqlExpr.getChildren();
        if (CollectionUtils.isEmpty(children)){
            return null;
        }
        for (SQLObject child : children) {
            if (tClass.isAssignableFrom(child.getClass())){
                return (T) child;
            }
        }
        for (SQLObject child : children) {
            if (child instanceof SQLExpr){
                T found = findFirstChild((SQLExpr) child, tClass);
                if (found != null){
                    return found;
                }
            }
        }
        return null;
    }

    public static class FocusParam {
        private String sName;
        private String key;
        private BiFunction<String, String, String> m;

        public FocusParam(String sName, String key, BiFunction<String, String, String> m) {
            this.sName = sName;
            this.key = key;
            this.m = m;
        }

        public String getsName() {
            return sName;
        }

        public void setsName(String sName) {
            this.sName = sName;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public BiFunction<String, String, String> getM() {
            return m;
        }

        public void setM(BiFunction<String, String, String> m) {
            this.m = m;
        }
    }

    public static class Result {
        private String replacedSql;
        private List<SelectProperty> selectProperties;
        private SQLStatement sqlStatement;


        public SQLStatement getSqlStatement() {
            return sqlStatement;
        }

        public void setSqlStatement(SQLStatement sqlStatement) {
            this.sqlStatement = sqlStatement;
        }

        public String getReplacedSql() {
            return replacedSql;
        }

        public void setReplacedSql(String replacedSql) {
            this.replacedSql = replacedSql;
        }

        public List<SelectProperty> getSelectProperties() {
            return selectProperties;
        }

        public void setSelectProperties(List<SelectProperty> selectProperties) {
            this.selectProperties = selectProperties;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "replacedSql='" + replacedSql + '\'' +
                    ", selectProperties=" + selectProperties +
                    '}';
        }
    }

    private static class BetweenProperty extends ConditionProperty {
    }

    private static class NotInSubQueryProperty extends ConditionProperty {
    }

    private static class InSubQueryProperty extends ConditionProperty {
    }

    private static class NotInProperty extends ConditionProperty {
    }

    private static class InProperty extends ConditionProperty {
    }

    private static class ConditionProperty extends Property {
    }

    private static class BinaryOpConditionProperty extends ConditionProperty {

        private boolean left;

        public boolean isLeft() {
            return left;
        }

        public void setLeft(boolean left) {
            this.left = left;
        }
    }

    private static class SelectProperty extends Property {

        private String alias;

        private boolean root;

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public String getAlias() {
            return alias;
        }

        public boolean isRoot() {
            return root;
        }

        public void setRoot(boolean root) {
            this.root = root;
        }
    }

    private static class Property {

        private SQLObject item;
        /**
         * tablealias the property belongs to
         */
        private String owner;

        private String name;

        /**
         * 解析出的tablename
         */
        private String tableName;


        private String column;

        private SQLObject bottomItem;

        public String getColumn() {
            return column;
        }

        public void setColumn(String column) {
            this.column = column;
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

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public SQLObject getBottomItem() {
            return bottomItem;
        }

        public void setBottomItem(SQLObject bottomItem) {
            this.bottomItem = bottomItem;
        }

        @Override
        public String toString() {
            return "Property{" +
                    "name='" + name + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", column='" + column + '\'' +
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

        private final List<SQLJoinTableSource> joinTableSources = new ArrayList<>();

        private final List<SQLBetweenExpr> sqlBetweenExprs = new ArrayList<>();

        private final List<SQLInListExpr> inListCondition = new ArrayList<>();

        private final List<SQLInSubQueryExpr> inSubQueryCondition = new ArrayList<>();

        private final List<SQLSelectQueryBlock> selectQueryBlocks = new ArrayList<>();

        private final List<SQLSelectItem> selectItems = new ArrayList<>();


        @Override
        public boolean visit(SQLExprTableSource x) {
            tableSources.add(x);
            return true;
        }

        @Override
        public boolean visit(SQLJoinTableSource x) {
            joinTableSources.add(x);
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
        public boolean visit(SQLInSubQueryExpr x) {
            inSubQueryCondition.add(x);
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


        @Override
        public boolean visit(SQLSelectItem x) {
            selectItems.add(x);
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

        public List<SQLJoinTableSource> getJoinTableSources() {
            return joinTableSources;
        }

        public List<SQLSelectItem> getSelectItems() {
            return selectItems;
        }

        public List<SQLInSubQueryExpr> getInSubQueryCondition() {
            return inSubQueryCondition;
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
