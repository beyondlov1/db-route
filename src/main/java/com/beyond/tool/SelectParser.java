package com.beyond.tool;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
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

    public static Result parse(String sql, Map<String, Map<String,String>> focusColumns) throws IOException {

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
        List<SQLSelectItem> allSelectItems = visitor.getSelectItems();
        for (SQLSelectItem selectItem : allSelectItems) {
            SelectProperty property = new SelectProperty();
            SQLExpr expr = selectItem.getExpr();
            if (expr instanceof SQLPropertyExpr){
                property.setName(((SQLPropertyExpr) expr).getName());
                property.setOwner(((SQLPropertyExpr) expr).getOwnerName());
            }else{
                property.setName(expr.toString());
                property.setOwner(null);
            }
            property.setAlias(selectItem.getAlias());
            property.setItem(selectItem);
            property.setBottomItem(selectItem);
            property.setRoot(rootSelectItems.contains(selectItem));
            properties.add(property);
        }


        // 处理equal
        List<SQLBinaryOpExpr> equalConditions = visitor.getBinaryOpExprs().stream().filter(x -> x.getOperator() == SQLBinaryOperator.Equality).collect(Collectors.toList());
        for (SQLBinaryOpExpr equalCondition : equalConditions) {
            if (equalCondition.getLeft() instanceof SQLPropertyExpr) {
                if (equalCondition.getRight() instanceof SQLIntegerExpr) {
                    InIntProperty inIntProperty = new InIntProperty();
                    inIntProperty.setOwner(((SQLPropertyExpr) equalCondition.getLeft()).getOwnerName());
                    inIntProperty.setName(((SQLPropertyExpr) equalCondition.getLeft()).getName());

                    inIntProperty.getValues().add((Integer) ((SQLIntegerExpr) equalCondition.getRight()).getValue());
                    inIntProperty.setItem(equalCondition);
                    properties.add(inIntProperty);
                }
            }

            if (equalCondition.getRight() instanceof SQLPropertyExpr) {
                if (equalCondition.getLeft() instanceof SQLIntegerExpr) {
                    InIntProperty inIntProperty = new InIntProperty();
                    inIntProperty.setOwner(((SQLPropertyExpr) equalCondition.getRight()).getOwnerName());
                    inIntProperty.setName(((SQLPropertyExpr) equalCondition.getRight()).getName());

                    inIntProperty.getValues().add((Integer) ((SQLIntegerExpr) equalCondition.getLeft()).getValue());
                    inIntProperty.setItem(equalCondition);
                    properties.add(inIntProperty);
                }
            }

            if (equalCondition.getLeft() instanceof SQLIdentifierExpr) {
                if (equalCondition.getRight() instanceof SQLIntegerExpr) {
                    InIntProperty inIntProperty = new InIntProperty();
                    inIntProperty.setOwner(null);
                    inIntProperty.setName(((SQLIdentifierExpr) equalCondition.getLeft()).getName());

                    inIntProperty.getValues().add((Integer) ((SQLIntegerExpr) equalCondition.getRight()).getValue());
                    inIntProperty.setItem(equalCondition);
                    properties.add(inIntProperty);
                }
            }


            if (equalCondition.getRight() instanceof SQLIdentifierExpr) {
                if (equalCondition.getLeft() instanceof SQLIntegerExpr) {
                    InIntProperty inIntProperty = new InIntProperty();
                    inIntProperty.setOwner(null);
                    inIntProperty.setName(((SQLIdentifierExpr) equalCondition.getRight()).getName());

                    inIntProperty.getValues().add((Integer) ((SQLIntegerExpr) equalCondition.getLeft()).getValue());
                    inIntProperty.setItem(equalCondition);
                    properties.add(inIntProperty);
                }
            }
        }

        // 处理 not equal
        List<SQLBinaryOpExpr> notEqualConditions = visitor.getBinaryOpExprs().stream().filter(x -> x.getOperator() == SQLBinaryOperator.NotEqual).collect(Collectors.toList());
        for (SQLBinaryOpExpr notEqualCondition : notEqualConditions) {
            if (notEqualCondition.getLeft() instanceof SQLPropertyExpr) {
                if (notEqualCondition.getRight() instanceof SQLIntegerExpr) {
                    NotInIntProperty notInIntProperty = new NotInIntProperty();
                    notInIntProperty.setOwner(((SQLPropertyExpr) notEqualCondition.getLeft()).getOwnerName());
                    notInIntProperty.setName(((SQLPropertyExpr) notEqualCondition.getLeft()).getName());

                    notInIntProperty.getValues().add((Integer) ((SQLIntegerExpr) notEqualCondition.getRight()).getValue());
                    notInIntProperty.setItem(notEqualCondition);
                    properties.add(notInIntProperty);
                }
            }

            if (notEqualCondition.getRight() instanceof SQLPropertyExpr) {
                if (notEqualCondition.getLeft() instanceof SQLIntegerExpr) {
                    NotInIntProperty notInIntProperty = new NotInIntProperty();
                    notInIntProperty.setOwner(((SQLPropertyExpr) notEqualCondition.getRight()).getOwnerName());
                    notInIntProperty.setName(((SQLPropertyExpr) notEqualCondition.getRight()).getName());

                    notInIntProperty.getValues().add((Integer) ((SQLIntegerExpr) notEqualCondition.getLeft()).getValue());
                    notInIntProperty.setItem(notEqualCondition);
                    properties.add(notInIntProperty);
                }
            }

            if (notEqualCondition.getLeft() instanceof SQLIdentifierExpr) {
                if (notEqualCondition.getRight() instanceof SQLIntegerExpr) {
                    NotInIntProperty notInIntProperty = new NotInIntProperty();
                    notInIntProperty.setOwner(null);
                    notInIntProperty.setName(((SQLIdentifierExpr) notEqualCondition.getLeft()).getName());

                    notInIntProperty.getValues().add((Integer) ((SQLIntegerExpr) notEqualCondition.getRight()).getValue());
                    notInIntProperty.setItem(notEqualCondition);
                    properties.add(notInIntProperty);
                }
            }


            if (notEqualCondition.getRight() instanceof SQLIdentifierExpr) {
                if (notEqualCondition.getLeft() instanceof SQLIntegerExpr) {
                    NotInIntProperty notInIntProperty = new NotInIntProperty();
                    notInIntProperty.setOwner(null);
                    notInIntProperty.setName(((SQLIdentifierExpr) notEqualCondition.getRight()).getName());

                    notInIntProperty.getValues().add((Integer) ((SQLIntegerExpr) notEqualCondition.getLeft()).getValue());
                    notInIntProperty.setItem(notEqualCondition);
                    properties.add(notInIntProperty);
                }
            }
        }

        // 处理 in
        for (SQLInListExpr inListExpr : visitor.getInListCondition()) {
            if (inListExpr.getExpr() instanceof SQLPropertyExpr) {
                if (inListExpr.isNot()) {
                    NotInIntProperty notInIntProperty = new NotInIntProperty();
                    notInIntProperty.setOwner(((SQLPropertyExpr) inListExpr.getExpr()).getOwnerName());
                    notInIntProperty.setName(((SQLPropertyExpr) inListExpr.getExpr()).getName());

                    List<SQLExpr> targetList = inListExpr.getTargetList();
                    for (SQLExpr sqlExpr : targetList) {
                        if (sqlExpr instanceof SQLIntegerExpr) {
                            Integer value = (Integer) ((SQLIntegerExpr) sqlExpr).getValue();
                            notInIntProperty.getValues().add(value);
                        }
                    }
                    notInIntProperty.setItem(inListExpr);
                    properties.add(notInIntProperty);
                } else {
                    InIntProperty inIntProperty = new InIntProperty();
                    inIntProperty.setOwner(((SQLPropertyExpr) inListExpr.getExpr()).getOwnerName());
                    inIntProperty.setName(((SQLPropertyExpr) inListExpr.getExpr()).getName());

                    List<SQLExpr> targetList = inListExpr.getTargetList();
                    for (SQLExpr sqlExpr : targetList) {
                        if (sqlExpr instanceof SQLIntegerExpr) {
                            Integer value = (Integer) ((SQLIntegerExpr) sqlExpr).getValue();
                            inIntProperty.getValues().add(value);
                        }
                    }
                    inIntProperty.setItem(inListExpr);
                    properties.add(inIntProperty);
                }

            }

            if (inListExpr.getExpr() instanceof SQLIdentifierExpr) {
                if (inListExpr.isNot()) {
                    NotInIntProperty notInIntProperty = new NotInIntProperty();
                    notInIntProperty.setOwner(null);
                    notInIntProperty.setName(((SQLIdentifierExpr) inListExpr.getExpr()).getName());

                    List<SQLExpr> targetList = inListExpr.getTargetList();
                    for (SQLExpr sqlExpr : targetList) {
                        if (sqlExpr instanceof SQLIntegerExpr) {
                            Integer value = (Integer) ((SQLIntegerExpr) sqlExpr).getValue();
                            notInIntProperty.getValues().add(value);
                        }
                    }
                    notInIntProperty.setItem(inListExpr);
                    properties.add(notInIntProperty);
                } else {
                    InIntProperty inIntProperty = new InIntProperty();
                    inIntProperty.setOwner(null);
                    inIntProperty.setName(((SQLIdentifierExpr) inListExpr.getExpr()).getName());

                    List<SQLExpr> targetList = inListExpr.getTargetList();
                    for (SQLExpr sqlExpr : targetList) {
                        if (sqlExpr instanceof SQLIntegerExpr) {
                            Integer value = (Integer) ((SQLIntegerExpr) sqlExpr).getValue();
                            inIntProperty.getValues().add(value);
                        }
                    }
                    inIntProperty.setItem(inListExpr);
                    properties.add(inIntProperty);
                }
            }
        }


        // 处理 > < ?
        List<SQLBinaryOpExpr> gtConditions = visitor.getBinaryOpExprs().stream()
                .filter(x -> x.getOperator() == SQLBinaryOperator.GreaterThan).collect(Collectors.toList());
        for (SQLBinaryOpExpr gtCondition : gtConditions) {
            SQLExpr left = gtCondition.getLeft();
            if (left instanceof SQLPropertyExpr) {
                SQLExpr right = gtCondition.getRight();
                if (right instanceof SQLIntegerExpr) {
                    RangeIntProperty rangeIntProperty = new RangeIntProperty();
                    rangeIntProperty.setOwner(((SQLPropertyExpr) left).getOwnerName());
                    rangeIntProperty.setName(((SQLPropertyExpr) left).getName());

                    rangeIntProperty.setItem(gtCondition);
                    rangeIntProperty.setLow((Integer) ((SQLIntegerExpr) right).getValue());
                    rangeIntProperty.setIncludeLow(false);
                    properties.add(rangeIntProperty);
                }
            }
        }

        List<SQLBinaryOpExpr> geConditions = visitor.getBinaryOpExprs().stream()
                .filter(x -> x.getOperator() == SQLBinaryOperator.GreaterThanOrEqual).collect(Collectors.toList());
        for (SQLBinaryOpExpr geCondition : geConditions) {
            SQLExpr left = geCondition.getLeft();
            if (left instanceof SQLPropertyExpr) {
                SQLExpr right = geCondition.getRight();
                if (right instanceof SQLIntegerExpr) {
                    RangeIntProperty rangeIntProperty = new RangeIntProperty();
                    rangeIntProperty.setOwner(((SQLPropertyExpr) left).getOwnerName());
                    rangeIntProperty.setName(((SQLPropertyExpr) left).getName());

                    rangeIntProperty.setItem(geCondition);
                    rangeIntProperty.setLow((Integer) ((SQLIntegerExpr) right).getValue());
                    rangeIntProperty.setIncludeLow(true);
                    properties.add(rangeIntProperty);
                }
            }
        }


        List<SQLBinaryOpExpr> ltConditions = visitor.getBinaryOpExprs().stream()
                .filter(x -> x.getOperator() == SQLBinaryOperator.LessThan).collect(Collectors.toList());
        for (SQLBinaryOpExpr ltCondition : ltConditions) {
            SQLExpr left = ltCondition.getLeft();
            if (left instanceof SQLPropertyExpr) {
                SQLExpr right = ltCondition.getRight();
                if (right instanceof SQLIntegerExpr) {
                    RangeIntProperty rangeIntProperty = new RangeIntProperty();
                    rangeIntProperty.setOwner(((SQLPropertyExpr) left).getOwnerName());
                    rangeIntProperty.setName(((SQLPropertyExpr) left).getName());

                    rangeIntProperty.setItem(ltCondition);
                    rangeIntProperty.setHigh((Integer) ((SQLIntegerExpr) right).getValue());
                    rangeIntProperty.setIncludeHigh(false);
                    properties.add(rangeIntProperty);
                }
            }
        }

        List<SQLBinaryOpExpr> leConditions = visitor.getBinaryOpExprs().stream()
                .filter(x -> x.getOperator() == SQLBinaryOperator.LessThanOrEqual).collect(Collectors.toList());
        for (SQLBinaryOpExpr leCondition : leConditions) {
            SQLExpr left = leCondition.getLeft();
            if (left instanceof SQLPropertyExpr) {
                SQLExpr right = leCondition.getRight();
                if (right instanceof SQLIntegerExpr) {
                    RangeIntProperty rangeIntProperty = new RangeIntProperty();
                    rangeIntProperty.setOwner(((SQLPropertyExpr) left).getOwnerName());
                    rangeIntProperty.setName(((SQLPropertyExpr) left).getName());

                    rangeIntProperty.setItem(leCondition);
                    rangeIntProperty.setHigh((Integer) ((SQLIntegerExpr) right).getValue());
                    rangeIntProperty.setIncludeHigh(true);
                    properties.add(rangeIntProperty);
                }
            }
        }

        // 处理 between ?
        List<SQLBetweenExpr> betweenConditions = visitor.getSqlBetweenExprs();
        for (SQLBetweenExpr betweenCondition : betweenConditions) {
            SQLExpr testExpr = betweenCondition.getTestExpr();
            if (testExpr instanceof SQLPropertyExpr) {
                RangeIntProperty rangeIntProperty = new RangeIntProperty();
                rangeIntProperty.setOwner(((SQLPropertyExpr) testExpr).getOwnerName());
                rangeIntProperty.setName(((SQLPropertyExpr) testExpr).getName());

                rangeIntProperty.setItem(betweenCondition);
                SQLExpr left = betweenCondition.getBeginExpr();
                if (left instanceof SQLIntegerExpr) {
                    rangeIntProperty.setLow((Integer) ((SQLIntegerExpr) left).getValue());
                    rangeIntProperty.setIncludeLow(true);
                }
                SQLExpr right = betweenCondition.getEndExpr();
                if (right instanceof SQLIntegerExpr) {
                    rangeIntProperty.setHigh((Integer) ((SQLIntegerExpr) right).getValue());
                    rangeIntProperty.setIncludeHigh(true);
                }
                properties.add(rangeIntProperty);
            }
        }


        List<SQLBinaryOpExpr> joinConditions = new ArrayList<>();
        List<SQLJoinTableSource> joinTableSources = visitor.getJoinTableSources();
        for (SQLJoinTableSource joinTableSource : joinTableSources) {
            SQLExpr condition = joinTableSource.getCondition();
            findChildren(condition, SQLBinaryOpExpr.class, joinConditions);
        }

        for (SQLBinaryOpExpr joinCondition : joinConditions) {
            SQLExpr left = joinCondition.getLeft();
            if (left instanceof SQLPropertyExpr){
                ConditionProperty conditionProperty = new ConditionProperty();
                conditionProperty.setItem(joinCondition);
                conditionProperty.setOwner(((SQLPropertyExpr) left).getOwnerName());
                conditionProperty.setName(((SQLPropertyExpr) left).getName());
                properties.add(conditionProperty);
            }

            if (left instanceof SQLIdentifierExpr){
                ConditionProperty conditionProperty = new ConditionProperty();
                conditionProperty.setItem(joinCondition);
                conditionProperty.setOwner(null);
                conditionProperty.setName(((SQLIdentifierExpr) left).getName());
                properties.add(conditionProperty);
            }
            SQLExpr right = joinCondition.getRight();
            if (right instanceof SQLPropertyExpr){
                ConditionProperty conditionProperty = new ConditionProperty();
                conditionProperty.setItem(joinCondition);
                conditionProperty.setOwner(((SQLPropertyExpr) right).getOwnerName());
                conditionProperty.setName(((SQLPropertyExpr) right).getName());
                properties.add(conditionProperty);
            }

            if (right instanceof SQLIdentifierExpr){
                ConditionProperty conditionProperty = new ConditionProperty();
                conditionProperty.setItem(joinCondition);
                conditionProperty.setOwner(null);
                conditionProperty.setName(((SQLIdentifierExpr) right).getName());
                properties.add(conditionProperty);
            }
        }

        properties.forEach(x -> {
            x.setName(org.apache.commons.lang3.StringUtils.strip(x.getName(), "`").toLowerCase());
            x.setColumn(x.getName());
            x.setBottomItem(x.getItem());
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
                    found = findByAliasRR(region, alias, name, property, tablesInRegion, region2Tables, alias2QueryBlock,nonAliasQueryBlocks);
                    if (found == null) {
                        // it is impossible
                        throw new RuntimeException();
                    }
                }
            } else {
                //检查表结构中的字段名,确认是哪个表的字段
                for (TableName tableName : tablesInRegion) {
                    Map<String, String> replaceMapping = focusColumns.get(tableName.getTableName());
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

        for (Property property : properties.stream().filter(x-> x.getBottomItem() instanceof SQLSelectItem).collect(Collectors.toList())) {
            Map<String, String> replaceMapping = focusColumns.get(property.getTableName());
            if (replaceMapping == null || !replaceMapping.containsKey(property.getColumn())){
                continue;
            }
            SQLSelectItem regionSelect = (SQLSelectItem) property.getBottomItem();
            if (regionSelect.getAlias() == null){
                regionSelect.setAlias(property.getColumn());
            }

            String encryptedName = replaceMapping.get(property.getColumn());
            if (regionSelect.getExpr() instanceof SQLIdentifierExpr){
                ((SQLIdentifierExpr) regionSelect.getExpr()).setName(encryptedName);
            }

            if (regionSelect.getExpr() instanceof SQLPropertyExpr){
                ((SQLPropertyExpr) regionSelect.getExpr()).setName(encryptedName);
            }
        }

        for (Property property : properties.stream().filter(x-> x.getBottomItem() instanceof SQLBinaryOpExpr).collect(Collectors.toList())) {
            Map<String, String> replaceMapping = focusColumns.get(property.getTableName());
            if (replaceMapping == null || !replaceMapping.containsKey(property.getColumn()) ){
                continue;
            }

            String encryptedName = replaceMapping.get(property.getColumn());
            SQLBinaryOpExpr condition = (SQLBinaryOpExpr) property.getBottomItem();

            SQLPropertyExpr sqlPropertyExpr = findFirstChild(condition, SQLPropertyExpr.class);
            if (sqlPropertyExpr!= null){
                sqlPropertyExpr.setName(encryptedName);
                continue;
            }

            SQLIdentifierExpr sqlIdentifierExpr = findFirstChild(condition, SQLIdentifierExpr.class);
            if (sqlIdentifierExpr!= null){
                sqlIdentifierExpr.setName(encryptedName);
            }
        }


        Result result = new Result();
        result.setSqlStatement(sqlStatement);
        result.setReplacedSql(sqlStatement.toString());
        result.setSelectProperties(properties.stream().filter(x->x instanceof SelectProperty).map(x->(SelectProperty)x).filter(SelectProperty::isRoot).collect(Collectors.toList()));

        return result;
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



    /**
     * 根据别名和要查询的字段名递归查询, 为了处理临时表的情况
     *
     * @param alias            条件中表的别名
     * @param propName         条件中的字段名
     * @param list             当前域下的表信息 (不包括临时表)
     * @param region2Tables    域对应的表  (不包括临时表)
     * @param alias2QueryBlock 别名->别名所在域
     * @param nonAliasQueryBlocks
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

    public static class Result{
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

    private static class RangeIntProperty extends ConditionProperty {
        private Integer low;
        private Integer high;
        private boolean includeLow;
        private boolean includeHigh;

        public Integer getLow() {
            return low;
        }

        public void setLow(Integer low) {
            this.low = low;
        }

        public Integer getHigh() {
            return high;
        }

        public void setHigh(Integer high) {
            this.high = high;
        }

        public boolean isIncludeLow() {
            return includeLow;
        }

        public void setIncludeLow(boolean includeLow) {
            this.includeLow = includeLow;
        }

        public boolean isIncludeHigh() {
            return includeHigh;
        }

        public void setIncludeHigh(boolean includeHigh) {
            this.includeHigh = includeHigh;
        }
    }

    private static class NotInIntProperty extends ConditionProperty {
        private List<Integer> values = new ArrayList<>();

        public List<Integer> getValues() {
            return values;
        }

        public void setValues(List<Integer> values) {
            this.values = values;
        }
    }

    private static class InIntProperty extends ConditionProperty {
        private List<Integer> values = new ArrayList<>();

        public List<Integer> getValues() {
            return values;
        }

        public void setValues(List<Integer> values) {
            this.values = values;
        }
    }

    private static class ConditionProperty extends Property{
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
    
    private static class Property{

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
