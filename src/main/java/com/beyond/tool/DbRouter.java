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
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author beyond
 * @date 2021/01/25
 */
public class DbRouter {

    public static List<MergedRoutingSource> parse(String sql, Map<String, List<String>> focusColumns) throws IOException {

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

        List<Property> properties = new ArrayList<>();

        // 处理equal
        List<SQLBinaryOpExpr> equalConditions = visitor.getBinaryOpExprs().stream().filter(x -> x.getOperator() == SQLBinaryOperator.Equality).collect(Collectors.toList());
        for (SQLBinaryOpExpr equalCondition : equalConditions) {
            if (equalCondition.getLeft() instanceof SQLPropertyExpr) {
                if (equalCondition.getRight() instanceof SQLIntegerExpr) {
                    InIntProperty inIntProperty = new InIntProperty();
                    inIntProperty.setOwner(((SQLPropertyExpr) equalCondition.getLeft()).getOwnerName());
                    inIntProperty.setName(((SQLPropertyExpr) equalCondition.getLeft()).getName());
                    inIntProperty.getValues().add((Integer) ((SQLIntegerExpr) equalCondition.getRight()).getValue());
                    inIntProperty.setCondition(equalCondition);
                    properties.add(inIntProperty);
                }
            }

            if (equalCondition.getRight() instanceof SQLPropertyExpr) {
                if (equalCondition.getLeft() instanceof SQLIntegerExpr) {
                    InIntProperty inIntProperty = new InIntProperty();
                    inIntProperty.setOwner(((SQLPropertyExpr) equalCondition.getRight()).getOwnerName());
                    inIntProperty.setName(((SQLPropertyExpr) equalCondition.getRight()).getName());
                    inIntProperty.getValues().add((Integer) ((SQLIntegerExpr) equalCondition.getLeft()).getValue());
                    inIntProperty.setCondition(equalCondition);
                    properties.add(inIntProperty);
                }
            }

            if (equalCondition.getLeft() instanceof SQLIdentifierExpr) {
                if (equalCondition.getRight() instanceof SQLIntegerExpr) {
                    InIntProperty inIntProperty = new InIntProperty();
                    inIntProperty.setOwner(null);
                    inIntProperty.setName(((SQLIdentifierExpr) equalCondition.getLeft()).getName());
                    inIntProperty.getValues().add((Integer) ((SQLIntegerExpr) equalCondition.getRight()).getValue());
                    inIntProperty.setCondition(equalCondition);
                    properties.add(inIntProperty);
                }
            }


            if (equalCondition.getRight() instanceof SQLIdentifierExpr) {
                if (equalCondition.getLeft() instanceof SQLIntegerExpr) {
                    InIntProperty inIntProperty = new InIntProperty();
                    inIntProperty.setOwner(null);
                    inIntProperty.setName(((SQLIdentifierExpr) equalCondition.getRight()).getName());
                    inIntProperty.getValues().add((Integer) ((SQLIntegerExpr) equalCondition.getLeft()).getValue());
                    inIntProperty.setCondition(equalCondition);
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
                    notInIntProperty.setCondition(notEqualCondition);
                    properties.add(notInIntProperty);
                }
            }

            if (notEqualCondition.getRight() instanceof SQLPropertyExpr) {
                if (notEqualCondition.getLeft() instanceof SQLIntegerExpr) {
                    NotInIntProperty notInIntProperty = new NotInIntProperty();
                    notInIntProperty.setOwner(((SQLPropertyExpr) notEqualCondition.getRight()).getOwnerName());
                    notInIntProperty.setName(((SQLPropertyExpr) notEqualCondition.getRight()).getName());
                    notInIntProperty.getValues().add((Integer) ((SQLIntegerExpr) notEqualCondition.getLeft()).getValue());
                    notInIntProperty.setCondition(notEqualCondition);
                    properties.add(notInIntProperty);
                }
            }

            if (notEqualCondition.getLeft() instanceof SQLIdentifierExpr) {
                if (notEqualCondition.getRight() instanceof SQLIntegerExpr) {
                    NotInIntProperty notInIntProperty = new NotInIntProperty();
                    notInIntProperty.setOwner(null);
                    notInIntProperty.setName(((SQLIdentifierExpr) notEqualCondition.getLeft()).getName());
                    notInIntProperty.getValues().add((Integer) ((SQLIntegerExpr) notEqualCondition.getRight()).getValue());
                    notInIntProperty.setCondition(notEqualCondition);
                    properties.add(notInIntProperty);
                }
            }


            if (notEqualCondition.getRight() instanceof SQLIdentifierExpr) {
                if (notEqualCondition.getLeft() instanceof SQLIntegerExpr) {
                    NotInIntProperty notInIntProperty = new NotInIntProperty();
                    notInIntProperty.setOwner(null);
                    notInIntProperty.setName(((SQLIdentifierExpr) notEqualCondition.getRight()).getName());
                    notInIntProperty.getValues().add((Integer) ((SQLIntegerExpr) notEqualCondition.getLeft()).getValue());
                    notInIntProperty.setCondition(notEqualCondition);
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
                    notInIntProperty.setCondition(inListExpr);
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
                    inIntProperty.setCondition(inListExpr);
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
                    notInIntProperty.setCondition(inListExpr);
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
                    inIntProperty.setCondition(inListExpr);
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
                    rangeIntProperty.setCondition(gtCondition);
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
                    rangeIntProperty.setCondition(geCondition);
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
                    rangeIntProperty.setCondition(ltCondition);
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
                    rangeIntProperty.setCondition(leCondition);
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
                rangeIntProperty.setCondition(betweenCondition);
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

        properties.forEach(x -> {
            x.setName(org.apache.commons.lang3.StringUtils.strip(x.getName(), "`").toLowerCase());
        });

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

        List<RoutingSource> routingSources = new ArrayList<>();
        for (Property property : properties) {
            TableName found = null;
            String alias = property.getOwner();
            String name = property.getName();
            SQLSelectQueryBlock region = findFirstParent(property.getCondition(), SQLSelectQueryBlock.class);
            List<TableName> tablesInRegion = region2Tables.get(region);
            if (alias != null) {
                found = findByAliasR(alias, tablesInRegion, region2Tables);
                if (found == null) {
                    // it is impossible, except a.order_id = 2343
                    found = findByAliasRR(region, alias, name, tablesInRegion, region2Tables, alias2QueryBlock);
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
                RoutingSource routingSource = new RoutingSource();
                routingSource.setTableName(found);
                routingSource.setTable(found.getTableName());
                routingSource.setPropName(name);
                routingSource.setRegion(region);
                routingSource.setCondition(property.getCondition());
                if (property instanceof InIntProperty) {
                    routingSource.setValues(((InIntProperty) property).getValues());
                }
                if (property instanceof NotInIntProperty) {
                    List<Integer> values = ((NotInIntProperty) property).getValues();
                    Collections.sort(values);
                    int low = Integer.MIN_VALUE;
                    for (Integer value : values) {
                        routingSource.getRanges().add(new Range(low, value - 1));
                        low = value + 1;
                    }
                    routingSource.getRanges().add(new Range(low, Integer.MAX_VALUE));
                }
                if (property instanceof RangeIntProperty) {
                    routingSource.getRanges().add(
                            new Range(((RangeIntProperty) property).getLow(), ((RangeIntProperty) property).isIncludeLow(),
                                    ((RangeIntProperty) property).getHigh(), ((RangeIntProperty) property).isIncludeHigh()));
                }
                routingSources.add(routingSource);
            }
        }

        // 过滤掉selectItem中的条件
        routingSources = routingSources.stream().filter(x -> {
            SQLObject condition = x.getCondition();
            SQLSelectItem belongSelectItem = findFirstParent(condition, SQLSelectItem.class);
            return belongSelectItem == null;
        }).collect(Collectors.toList());

        List<MergedRoutingSource> logicMergedRoutingSources = new ArrayList<>();
        List<ConditionKey> conditionKeys = new ArrayList<>();
        for (RoutingSource routingSource : routingSources) {
            ConditionKey found = null;
            ConditionKey key = getKey(routingSource);
            for (ConditionKey conditionKey : conditionKeys) {
                if (Objects.equals(conditionKey, key)) {
                    found = conditionKey;
                    break;
                }
            }

            if (found == null) {
                conditionKeys.add(key);
            }
        }

        for (ConditionKey conditionKey : conditionKeys) {
            Map<Integer, RoutingSource> routingSourceMap = routingSources.stream().filter(x -> Objects.equals(getKey(x), conditionKey))
                    .collect(Collectors.toMap(x -> System.identityHashCode(x.getCondition()), x -> x));
            logicMergedRoutingSources.add(merge(conditionKey.getRegion().getWhere(), routingSourceMap, conditionKey));
        }

        return logicMergedRoutingSources.stream().filter(Objects::nonNull).filter(x -> {
            List<String> columns = focusColumns.get(x.getTable());
            if (columns == null) {
                return false;
            } else {
                return columns.contains(x.getPropName());
            }
        }).collect(Collectors.toList());

    }

    private static MergedRoutingSource merge(SQLObject condition, Map<Integer, RoutingSource> routingSourceMap, ConditionKey conditionKey) {
        if (routingSourceMap.get(System.identityHashCode(condition)) != null) {
            MergedRoutingSource mergedRoutingSource = new MergedRoutingSource();
            mergedRoutingSource.setTable(conditionKey.getTable());
            mergedRoutingSource.setRegion(conditionKey.getRegion());
            mergedRoutingSource.setPropName(conditionKey.getPropName());
            RoutingSource routingSource = routingSourceMap.get(System.identityHashCode(condition));
            mergedRoutingSource.getValues().addAll(routingSource.getValues());
            if (CollectionUtils.isNotEmpty(routingSource.getRanges())) {
                mergedRoutingSource.getRanges().addAll(routingSource.getRanges());
            }
            return mergedRoutingSource;
        }
        if (condition instanceof SQLBinaryOpExpr) {
            SQLBinaryOperator operator = ((SQLBinaryOpExpr) condition).getOperator();
            MergedRoutingSource leftMerged = null;
            if (((SQLBinaryOpExpr) condition).getLeft() instanceof SQLBinaryOpExpr || ((SQLBinaryOpExpr) condition).getLeft() instanceof SQLInListExpr) {
                leftMerged = merge(((SQLBinaryOpExpr) condition).getLeft(), routingSourceMap, conditionKey);
            }

            MergedRoutingSource rightMerged = null;
            if (((SQLBinaryOpExpr) condition).getRight() instanceof SQLBinaryOpExpr || ((SQLBinaryOpExpr) condition).getRight() instanceof SQLInListExpr) {
                rightMerged = merge(((SQLBinaryOpExpr) condition).getRight(), routingSourceMap, conditionKey);
            }
            if (operator == SQLBinaryOperator.BooleanAnd) {
                return and(leftMerged, rightMerged, conditionKey);
            }
            if (operator == SQLBinaryOperator.BooleanOr) {
                return or(leftMerged, rightMerged, conditionKey);
            }
        }
        return null;
    }

    private static ConditionKey getKey(MergedRoutingSource mergedRoutingSource) {
        return new ConditionKey(mergedRoutingSource.getRegion(), mergedRoutingSource.getTable(), mergedRoutingSource.getPropName());
    }

    private static ConditionKey getKey(RoutingSource routingSource) {
        return new ConditionKey(routingSource.getRegion(), routingSource.getTable(), routingSource.getPropName());
    }

    public static MergedRoutingSource fromKey(ConditionKey conditionKey) {
        MergedRoutingSource mergedRoutingSource = new MergedRoutingSource();
        mergedRoutingSource.setTable(conditionKey.getTable());
        mergedRoutingSource.setRegion(conditionKey.getRegion());
        mergedRoutingSource.setPropName(conditionKey.getPropName());
        return mergedRoutingSource;
    }

    private static MergedRoutingSource and(MergedRoutingSource left, MergedRoutingSource right, ConditionKey conditionKey) {
        if (left == null && right == null) {
            return null;
        }
        MergedRoutingSource result = fromKey(conditionKey);
        if (left != null && Objects.equals(getKey(left), conditionKey)) {
            if (CollectionUtils.isNotEmpty(result.getValues()) && CollectionUtils.isNotEmpty(left.getValues())) {
                List<Integer> intersection = ListUtils.intersection(result.getValues(), left.getValues());
                result.setValues(intersection);
            } else {
                result.setValues(left.getValues());
            }
            result.setRanges(left.getRanges());
        }
        if (right != null && Objects.equals(getKey(right), conditionKey)) {
            if (CollectionUtils.isNotEmpty(result.getValues())) {
                List<Integer> intersection = ListUtils.intersection(result.getValues(), right.getValues());
                result.setValues(intersection);
            } else {
                result.setValues(right.getValues());
            }
            if (CollectionUtils.isEmpty(result.getRanges())) {
                result.setRanges(right.getRanges());
            } else {
                if (CollectionUtils.isNotEmpty(right.getRanges())) {
                    List<Range> ranges = new ArrayList<>();
                    for (Range range : result.getRanges()) {
                        for (Range rightRange : right.getRanges()) {
                            ranges.add(Range.intersection(rightRange, range));
                        }
                    }
                    result.setRanges(ranges);
                }
            }
        }

        // in 和 range 取交集
        if (CollectionUtils.isNotEmpty(result.getValues()) && CollectionUtils.isNotEmpty(result.getRanges())) {
            Iterator<Integer> iterator = result.getValues().iterator();
            while (iterator.hasNext()) {
                Integer next = iterator.next();
                boolean include = false;
                for (Range range : result.getRanges()) {
                    if (range.include(next)) {
                        include = true;
                        break;
                    }
                }
                if (!include) {
                    iterator.remove();
                }
            }
            result.getRanges().clear();
        }

        return result;
    }

    private static MergedRoutingSource or(MergedRoutingSource left, MergedRoutingSource right, ConditionKey conditionKey) {
        if (left == null && right == null) {
            return null;
        }
        MergedRoutingSource result = fromKey(conditionKey);
        if (left != null && Objects.equals(getKey(left), conditionKey)) {
            List<Integer> union = ListUtils.union(result.getValues(), left.getValues());
            result.setValues(union);
            result.setRanges(left.getRanges());
        }
        if (right != null && Objects.equals(getKey(right), conditionKey)) {
            List<Integer> union = ListUtils.union(result.getValues(), right.getValues());
            result.setValues(union);
            result.getRanges().addAll(right.getRanges());
        }
        return result;
    }

    /**
     * 根据别名和要查询的字段名递归查询, 为了处理临时表的情况
     *
     * @param alias            条件中表的别名
     * @param propName         条件中的字段名
     * @param list             当前域下的表信息 (不包括临时表)
     * @param region2Tables    域对应的表  (不包括临时表)
     * @param alias2QueryBlock 别名->别名所在域
     * @return 表信息
     */
    private static TableName findByAliasRR(SQLSelectQueryBlock region, String alias, String propName, List<TableName> list, Map<SQLSelectQueryBlock, List<TableName>> region2Tables,
                                           Map<TableAliasKey, SQLSelectQueryBlock> alias2QueryBlock) {
        TableName tableName = findByAliasR(alias, list, region2Tables);
        if (tableName == null) {
            SQLSelectQueryBlock foundVirtualTable = alias2QueryBlock.get(new TableAliasKey(alias, region));
            if (foundVirtualTable == null) {
                return null;
            }
            SQLSelectItem selectItem = foundVirtualTable.findSelectItem(propName);
            if (selectItem.getExpr() instanceof SQLPropertyExpr) {
                String nextAlias = ((SQLPropertyExpr) selectItem.getExpr()).getOwnerName();
                String nextPropName = ((SQLPropertyExpr) selectItem.getExpr()).getName();
                return findByAliasRR(foundVirtualTable,nextAlias, nextPropName, region2Tables.get(foundVirtualTable), region2Tables, alias2QueryBlock);
            }
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
     * @return
     */
    private static TableName findByAlias(String alias, List<TableName> list) {
        for (TableName tableName : list) {
            if (org.apache.commons.lang3.StringUtils.equals(tableName.getAlias(), alias)) {
                return tableName;
            }
            // 可能为别名写表名的情况, 如: ts_wholesale_order.id = 123.
            String[] split = StringUtils.split(tableName.getTableName(), "\\.");
            if (split.length == 1) {
                String simpleTableName = split[0];
                if (org.apache.commons.lang3.StringUtils.equals(simpleTableName, alias)) {
                    return tableName;
                }
            }
            if (split.length == 2) {
                String simpleTableName = split[1];
                if (org.apache.commons.lang3.StringUtils.equals(simpleTableName, alias)) {
                    return tableName;
                }
            }

        }
        return null;
    }

    private static class ConditionKey {
        private SQLSelectQueryBlock region;
        private String table;
        private String propName;

        public ConditionKey(SQLSelectQueryBlock region, String table, String propName) {
            this.region = region;
            this.table = table;
            this.propName = propName;
        }

        public SQLSelectQueryBlock getRegion() {
            return region;
        }

        public String getTable() {
            return table;
        }

        public String getPropName() {
            return propName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ConditionKey that = (ConditionKey) o;

            if (region != null ? !region.equals(that.region) : that.region != null) return false;
            if (table != null ? !table.equals(that.table) : that.table != null) return false;
            return propName != null ? propName.equals(that.propName) : that.propName == null;
        }

        @Override
        public int hashCode() {
            int result = region != null ? region.hashCode() : 0;
            result = 31 * result + (table != null ? table.hashCode() : 0);
            result = 31 * result + (propName != null ? propName.hashCode() : 0);
            return result;
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

    private static class RangeIntProperty extends Property {
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

    private static class NotInIntProperty extends Property {
        private List<Integer> values = new ArrayList<>();

        public List<Integer> getValues() {
            return values;
        }

        public void setValues(List<Integer> values) {
            this.values = values;
        }
    }


    private static class InIntProperty extends Property {
        private List<Integer> values = new ArrayList<>();

        public List<Integer> getValues() {
            return values;
        }

        public void setValues(List<Integer> values) {
            this.values = values;
        }
    }

    private static class Property {
        private String owner;
        private String name;
        private SQLObject condition;

        public SQLObject getCondition() {
            return condition;
        }

        public void setCondition(SQLObject condition) {
            this.condition = condition;
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
