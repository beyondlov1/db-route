package com.beyond.tool;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * @author chenshipeng
 * @date 2022/06/22
 */
public class DruidParseUtil {

    public static <T> void  findChildren(SQLExpr sqlExpr, Class<T> tClass, List<T> list) {
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

    public static  <T> T findFirstChild(SQLExpr sqlExpr, Class<T> tClass){
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

}
