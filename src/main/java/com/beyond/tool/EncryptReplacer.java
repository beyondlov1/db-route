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
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * @author beyond
 * @date 2021/01/25
 */
public class EncryptReplacer {


    /**
     * 解析update语句, 并更改sql, 增加加密列
     * @param sql 带?的sql: update db_ysb_order.ts_wholesale_order set phone = ? where phone in (?,?,?)
     * @param parameters  ? 参数列表
     * @param focusColumns 加密信息  Map<表, Map<列名, 加密信息>>
     * @return  update db_ysb_order.ts_wholesale_order set phone = ?,phone_m = ? where phone_m in (?,?,?)
     */
    public static UpdateResult replaceUpdate3(String sql, List<Object> parameters, Map<String, Map<String, FocusParam>> focusColumns) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);

        TableCollectVisitor visitor = new TableCollectVisitor();
        sqlStatement.accept(visitor);

        if (sqlStatement instanceof SQLUpdateStatement){
            String tablename = ((SQLUpdateStatement) sqlStatement).getTableSource().toString();
            Map<String, FocusParam> column2FocusParam = focusColumns.get(tablename);
            if (column2FocusParam == null){
                return null;
            }

            List<SQLUpdateSetItem> items = ((SQLUpdateStatement) sqlStatement).getItems();
            List<Object> newParameters = new ArrayList<>();
            Map<Integer, Object> toAddParameterMap = new TreeMap<>(Comparator.comparingInt(x-> (int) x).reversed());
            Map<Integer, SQLUpdateSetItem> toAddSetItems = new TreeMap<>(Comparator.comparingInt(x-> (int) x).reversed());
            Iterator<Object> iterator = parameters.iterator();
            int i = 0 , itemIndex = 0;
            for (SQLUpdateSetItem sqlUpdateSetItem : items) {
                SQLExpr column = sqlUpdateSetItem.getColumn();
                SQLExpr value = sqlUpdateSetItem.getValue();
                if ( value instanceof SQLVariantRefExpr){
                    Object parameter = iterator.next();
                    newParameters.add(parameter);
                    if (column instanceof SQLIdentifierExpr ){
                        String name = ((SQLIdentifierExpr) column).getName();
                        FocusParam focusParam = column2FocusParam.get(name);
                        if (focusParam != null){
                            SQLUpdateSetItem clone = sqlUpdateSetItem.clone();
                            SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr();
                            sqlIdentifierExpr.setParent(sqlStatement);
                            sqlIdentifierExpr.setName(focusParam.getsName());
                            clone.setColumn(sqlIdentifierExpr);
                            toAddSetItems.put(itemIndex + 1, clone);
                            String encryptedValue = focusParam.getM().apply(parameter.toString(), focusParam.getKey());
                            newParameters.add(encryptedValue);
                            toAddParameterMap.put(i+1, encryptedValue);
                        }
                    }
                    i++;
                }
                itemIndex ++;
            }

            for (Integer index : toAddSetItems.keySet()) {
                items.add(index, toAddSetItems.get(index));
            }

            Map<Integer, Object> toUpdateParameterMap = new HashMap<>();
            List<SQLVariantRefExpr> whereVariants = new ArrayList<>();
            Map<SQLIdentifierExpr, String> toUpdateNameMap = new HashMap<>();
            findChildren(((SQLUpdateStatement) sqlStatement).getWhere(), SQLVariantRefExpr.class, whereVariants);
            for (SQLVariantRefExpr whereVariant : whereVariants) {
                Object parameter = iterator.next();
                Set<SQLExpr> sqlExprs = new HashSet<>();
                sqlExprs.add(findFirstParent(whereVariant, SQLInListExpr.class));
                sqlExprs.add(findFirstParent(whereVariant, SQLBinaryOpExpr.class));
                for (SQLExpr sqlExpr : sqlExprs) {
                    if (sqlExpr != null){
                        SQLIdentifierExpr firstChild = findFirstChild(sqlExpr, SQLIdentifierExpr.class);
                        if (firstChild != null) {
                            String name = firstChild.getName();
                            FocusParam focusParam = column2FocusParam.get(name);
                            if (focusParam != null){
                                String encryptedValue = focusParam.getM().apply(parameter.toString(), focusParam.getKey());
                                newParameters.add(encryptedValue);
                                toUpdateParameterMap.put(i, encryptedValue);
                                toUpdateNameMap.put(firstChild, focusParam.getsName());
                            }
                        }
                    }
                }
                i++;
            }

            for (SQLIdentifierExpr sqlIdentifierExpr : toUpdateNameMap.keySet()) {
                sqlIdentifierExpr.setName(toUpdateNameMap.get(sqlIdentifierExpr));
            }

            System.out.println(sqlStatement.toString());
            return new UpdateResult(sqlStatement.toString(), newParameters, toAddParameterMap,toUpdateParameterMap);
        }

        return null;
    }

    /**
     * 解析insert语句, 并更改sql, 增加加密列
     * @param sql 带?的sql: insert into test.t_test (id, phone, name) values (?,?,?),(?,?,?),(?,?,9999)
     * @param parameters  ? 参数列表
     * @param focusColumns 加密信息  Map<表, Map<列名, 加密信息>>
     * @return  insert into test.t_test (id, phone, name, phone_m) values (?,?,?,?),(?,?,?,?),(?,?,9999,?)
     */
    public static InsertResult replaceInsert3(String sql, List<Object> parameters, Map<String, Map<String, FocusParam>> focusColumns) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);

        TableCollectVisitor visitor = new TableCollectVisitor();
        sqlStatement.accept(visitor);

        if (sqlStatement instanceof SQLInsertStatement){
            String tablename = ((SQLInsertStatement) sqlStatement).getTableSource().getExpr().toString();
            Map<String, FocusParam> column2FocusParam = focusColumns.get(tablename);
            if (column2FocusParam == null){
                return null;
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

            // 解析要增加的列名
            List<String> toAddColumnNames = new ArrayList<>();
            for (Integer index : toEncrypt.keySet()) {
                FocusParam toEncryptItem = toEncrypt.get(index);
                toAddColumnNames.add(toEncryptItem.getsName());
            }

            Map<Integer, Object> parameterMap = new TreeMap<>(Comparator.comparingInt(x-> (int) x).reversed());
            List<List<SQLVariantRefExpr>> toAddValueExprsList = new ArrayList<>();
            List<List<Object>> valuesClauses = new ArrayList<>();
            Iterator<Object> iterator = parameters.iterator();
            for (SQLInsertStatement.ValuesClause valuesClause : ((SQLInsertStatement) sqlStatement).getValuesList()) {
                List<Object> values = new ArrayList<>();
                for (SQLExpr value : valuesClause.getValues()) {
                    if (value instanceof SQLVariantRefExpr){
                        Object next = iterator.next();
                        values.add(next);
                    }
                    if (value instanceof SQLTextLiteralExpr) {
                        values.add(((SQLTextLiteralExpr) value).getText());
                    }
                    if (value instanceof SQLIntegerExpr) {
                        values.add(((SQLIntegerExpr) value).getValue());
                    }
                }
                valuesClauses.add(values);
            }
            int valIndex = 0;
            for (List<Object> values : valuesClauses) {
                List<SQLVariantRefExpr> toAddValueExprs = new ArrayList<>();
                for (Integer index : toEncrypt.keySet()) {
                    FocusParam toEncryptItem = toEncrypt.get(index);
                    Object value = values.get(index);
                    if (value instanceof String){
                        String encryptedValue = toEncryptItem.getM().apply(value.toString(), toEncryptItem.getKey());
                        parameterMap.put(valIndex + values.size(), encryptedValue);
                        values.add(encryptedValue);

                        SQLVariantRefExpr sqlVariantRefExpr = new SQLVariantRefExpr();
                        sqlVariantRefExpr.setName("?");
                        toAddValueExprs.add(sqlVariantRefExpr);
                    }
                    if (value instanceof Number){
                        String encryptedValue = toEncryptItem.getM().apply(value.toString(), toEncryptItem.getKey());
                        parameterMap.put(valIndex + values.size(), encryptedValue);
                        values.add(encryptedValue);

                        SQLVariantRefExpr sqlVariantRefExpr = new SQLVariantRefExpr();
                        sqlVariantRefExpr.setName("?");
                        toAddValueExprs.add(sqlVariantRefExpr);
                    }
                }
                toAddValueExprsList.add(toAddValueExprs);
                valIndex += values.size();
            }

            for (String toAddColumnName : toAddColumnNames) {
                SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr();
                sqlIdentifierExpr.setParent(sqlStatement);
                sqlIdentifierExpr.setName(toAddColumnName);
                ((SQLInsertStatement) sqlStatement).addColumn(sqlIdentifierExpr);
            }

            int j = 0;
            for (SQLInsertStatement.ValuesClause valuesClause : ((SQLInsertStatement) sqlStatement).getValuesList()) {
                List<SQLVariantRefExpr> sqlVariantRefExprs = toAddValueExprsList.get(j);
                for (SQLVariantRefExpr sqlVariantRefExpr : sqlVariantRefExprs) {
                    sqlVariantRefExpr.setParent(valuesClause);
                    valuesClause.addValue(sqlVariantRefExpr);
                }
                j++;
            }

            List<Object> newParameters = valuesClauses.stream().flatMap(Collection::stream).collect(Collectors.toList());
            System.out.println(newParameters);
            System.out.println(parameterMap);
            return new InsertResult(sqlStatement.toString(),newParameters,parameterMap);
        }

        return null;
    }

    /**
     * 添加加密字段
     * @param sql insert into test.t_test (id, phone, name) values (1223,122,1223)
     * @param focusColumns 加密信息  Map<表, Map<列名, 加密信息>>
     * @return insert into test.t_test (id, phone, name, phone_m) values (1223,122,1223,'122xxxx')
     */
    public static String replaceInsert2(String sql, Map<String, Map<String, FocusParam>> focusColumns) {
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
            // 增加列名
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
     * @param sql insert into test.t_test (id, phone, name) values (1223,122,1223)
     * @param focusColumns 加密信息  Map<表, Map<列名, 加密信息>>
     * @return insert into test.t_test (id, phone_m, name) values (1223,'122xxxx',1223)
     */
    public static String replaceInsert(String sql, Map<String, Map<String, FocusParam>> focusColumns) {
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
                    safeReplace(sqlExpr, new HashMap<>(), new HashMap<>(), focusParam);
                }
            }
        }
        return sqlStatement.toString();
    }

    /**
     * 解析select语句, 并更改sql, 增加加密列
     * @param sql 带?的sql: select id, phone from test.t_test where phone = ?
     * @param parameters  ? 参数列表
     * @param focusColumns 加密信息  Map<表, Map<列名, 加密信息>>
     * @return  select id, phone_m as phone from test.t_test where phone_m = ?
     */
    public static SelectResult replaceSelect3(String sql, List<Object> parameters, Map<String, Map<String, FocusParam>> focusColumns) {

        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);

        TableCollectVisitor visitor = new TableCollectVisitor();
        sqlStatement.accept(visitor);

        // 这个方法可以解析property中的owner属于哪个表, 在resolvedOwnerObj, 如果是子查询, 会是一个子查询的obj
        SchemaStatVisitor schemaStatVisitor = SQLUtils.createSchemaStatVisitor(DbType.mysql);
        sqlStatement.accept(schemaStatVisitor);

        Map<Integer, Pair<Integer, Object>> variantValueMap = new HashMap<>();
        List<SQLVariantRefExpr> variantRefExprs = visitor.getVariantRefExprs();
        int i = 0;
        for (SQLVariantRefExpr variantRefExpr : variantRefExprs) {
            variantValueMap.put(System.identityHashCode(variantRefExpr), Pair.of(i,parameters.get(i)));
            i++;
        }

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

        Map<Integer, Object> toUpdateParameterMap = new HashMap<>();
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
                    encryptProperty(property, variantValueMap,toUpdateParameterMap, focusParam);
                }
            }

            if (property.getBottomItem() instanceof SQLIdentifierExpr) {
                FocusParam focusParam = replaceMapping.get(property.getColumn());
                String encryptedName = focusParam.getsName();
                SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) property.getBottomItem();
                if (sqlIdentifierExpr != null) {
                    sqlIdentifierExpr.setName(encryptedName);
                    encryptProperty(property, variantValueMap,toUpdateParameterMap, focusParam);
                }
            }
        }


        List<Object> newParameters = new ArrayList<>();
        for (int j = 0; j < parameters.size(); j++) {
            Object o = toUpdateParameterMap.get(j);
            if (o != null){
                newParameters.add(o);
            }else{
                newParameters.add(parameters.get(j));
            }
        }

        SelectResult selectResult = new SelectResult();
        selectResult.setReplacedSql(sqlStatement.toString());
        selectResult.setToUpdateParameterMap(toUpdateParameterMap);
        selectResult.setNewParameters(newParameters);
        selectResult.setSelectProperties(properties.stream().filter(x -> x instanceof SelectProperty).map(x -> (SelectProperty) x).filter(SelectProperty::isRoot).collect(Collectors.toList()));

        return selectResult;
    }

    /**
     *
     * @param sql 不带?
     * @param focusColumns 加密信息  Map<表, Map<列名, 加密信息>>
     */
    public static SelectResult replaceSelect(String sql, Map<String, Map<String, FocusParam>> focusColumns) {
        return replaceSelect3(sql, Collections.emptyList(), focusColumns);
    }


    /*********************************************************************************************************************************************/


    private static void encryptProperty(Property property, Map<Integer, Pair<Integer, Object>> variantValueMap,Map<Integer, Object> toUpdateParameterMap, FocusParam focusParam) {
        if (property instanceof BinaryOpConditionProperty) {
            SQLBinaryOpExpr item = (SQLBinaryOpExpr) property.getItem();
            safeReplace(item.getLeft(),variantValueMap, toUpdateParameterMap, focusParam);
            safeReplace(item.getRight(), variantValueMap, toUpdateParameterMap, focusParam);
        }

        if (property instanceof BetweenProperty) {
            SQLBetweenExpr item = (SQLBetweenExpr) property.getItem();
            safeReplace(item.getBeginExpr(), variantValueMap, toUpdateParameterMap, focusParam);
            safeReplace(item.getEndExpr(),variantValueMap, toUpdateParameterMap, focusParam);
        }

        if (property instanceof InProperty) {
            SQLInListExpr item = (SQLInListExpr) property.getItem();
            List<SQLExpr> targetList = item.getTargetList();
            for (SQLExpr sqlExpr : targetList) {
                safeReplace(sqlExpr, variantValueMap,toUpdateParameterMap, focusParam);
            }
        }
    }

    private static void safeReplace(SQLExpr sqlExpr, Map<Integer, Pair<Integer, Object>> variantValueMap, Map<Integer, Object> toUpdateParameterMap, FocusParam focusParam) {
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

        if (sqlExpr instanceof SQLVariantRefExpr) {
            Pair<Integer, Object> parameterValue = variantValueMap.get(System.identityHashCode(sqlExpr));
            Object value = parameterValue.getRight();
            if (value != null) {
                String encrypted = focusParam.getM().apply(focusParam.getKey(), value.toString());
                toUpdateParameterMap.put(parameterValue.getLeft(),encrypted);
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

    public static class UpdateResult{
        /**
         * 替换后带?的sql
         */
        private String replacedSql;
        /**
         * 新的完整parameter列表
         */
        private List<Object> newParameters;
        /**
         * 要增加的parameter, <列表insertIndex, 值> , 这个map是有顺序的, 按insertIndex倒序, 使用时, 从大到小依次插入, 防止不断移位
         * 要在 toUpdateParameterMap 之后再使用, 防止移位
         */
        private Map<Integer, Object> toAddParameterMap;

        /**
         * 要替换的的parameter, 用于替换where中的?
         * 要在 toAddParameterMap 之前使用
         */
        private Map<Integer, Object> toUpdateParameterMap;

        public UpdateResult(String replacedSql, List<Object> newParameters, Map<Integer, Object> toAddParameterMap, Map<Integer, Object> toUpdateParameterMap) {
            this.replacedSql = replacedSql;
            this.newParameters = newParameters;
            this.toAddParameterMap = toAddParameterMap;
            this.toUpdateParameterMap = toUpdateParameterMap;
        }

        public String getReplacedSql() {
            return replacedSql;
        }

        public void setReplacedSql(String replacedSql) {
            this.replacedSql = replacedSql;
        }

        public List<Object> getNewParameters() {
            return newParameters;
        }

        public void setNewParameters(List<Object> newParameters) {
            this.newParameters = newParameters;
        }

        public Map<Integer, Object> getToAddParameterMap() {
            return toAddParameterMap;
        }

        public void setToAddParameterMap(Map<Integer, Object> toAddParameterMap) {
            this.toAddParameterMap = toAddParameterMap;
        }

        public Map<Integer, Object> getToUpdateParameterMap() {
            return toUpdateParameterMap;
        }

        public void setToUpdateParameterMap(Map<Integer, Object> toUpdateParameterMap) {
            this.toUpdateParameterMap = toUpdateParameterMap;
        }

        @Override
        public String toString() {
            return "UpdateResult{" +
                    "replacedSql='" + replacedSql + '\'' +
                    ", newParameters=" + newParameters +
                    ", toAddParameterMap=" + toAddParameterMap +
                    ", toUpdateParameterMap=" + toUpdateParameterMap +
                    '}';
        }
    }

    public static class InsertResult{
        /**
         * 替换后带?的sql
         */
        private String replacedSql;
        /**
         * 新的完整parameter列表
         */
        private List<Object> newParameters;
        /**
         * 要增加的parameter, <列表insertIndex, 值> , 这个map是有顺序的, 按insertIndex倒序, 使用时, 从大到小依次插入, 防止不断移位
         */
        private Map<Integer, Object> toAddParameterMap;

        public InsertResult(String replacedSql, List<Object> newParameters, Map<Integer, Object> toAddParameterMap) {
            this.replacedSql = replacedSql;
            this.newParameters = newParameters;
            this.toAddParameterMap = toAddParameterMap;
        }

        public String getReplacedSql() {
            return replacedSql;
        }

        public void setReplacedSql(String replacedSql) {
            this.replacedSql = replacedSql;
        }

        public List<Object> getNewParameters() {
            return newParameters;
        }

        public void setNewParameters(List<Object> newParameters) {
            this.newParameters = newParameters;
        }

        public Map<Integer, Object> getToAddParameterMap() {
            return toAddParameterMap;
        }

        public void setToAddParameterMap(Map<Integer, Object> toAddParameterMap) {
            this.toAddParameterMap = toAddParameterMap;
        }

        @Override
        public String toString() {
            return "InsertResult{" +
                    "replacedSql='" + replacedSql + '\'' +
                    ", newParameters=" + newParameters +
                    ", toAddParameterMap=" + toAddParameterMap +
                    '}';
        }
    }

    public static class SelectResult {
        private String replacedSql;
        private List<SelectProperty> selectProperties;
        private Map<Integer, Object> toUpdateParameterMap;
        private List<Object> newParameters;

        public List<Object> getNewParameters() {
            return newParameters;
        }

        public void setNewParameters(List<Object> newParameters) {
            this.newParameters = newParameters;
        }

        public Map<Integer, Object> getToUpdateParameterMap() {
            return toUpdateParameterMap;
        }

        public void setToUpdateParameterMap(Map<Integer, Object> toUpdateParameterMap) {
            this.toUpdateParameterMap = toUpdateParameterMap;
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
            return "SelectResult{" +
                    "replacedSql='" + replacedSql + '\'' +
                    ", selectProperties=" + selectProperties +
                    ", toUpdateParameterMap=" + toUpdateParameterMap +
                    ", newParameters=" + newParameters +
                    '}';
        }
    }

    public static class FocusParam {
        /**
         * 加密字段名
         */
        private String sName;
        /**
         * 加密key
         */
        private String key;

        /**
         * 加密方法 <target, key> -> 加密后
         */
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

        private final List<SQLVariantRefExpr> variantRefExprs = new ArrayList<>();


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

        @Override
        public boolean visit(SQLVariantRefExpr x) {
            variantRefExprs.add(x);
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

        public List<SQLVariantRefExpr> getVariantRefExprs() {
            return variantRefExprs;
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
