package com.beyond.tool;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.List;

/**
 * @author chenshipeng
 * @date 2022/06/20
 */
public class JSqlParser {
    public static void main(String[] args) throws JSQLParserException {
        Statement statement = CCJSqlParserUtil.parse("SELECT\n" +
                "                           t.phone as p,\n" +
                "       a.phone p2\n" +
                "                       FROM\n" +
                "                           db_ysb_order.ts_wholesale_order t\n" +
                "                       LEFT JOIN db_ysb_order.ts_wholesale_order_assess assess on assess.order_id = t.id\n" +
                "                        left join  db_ysb_user.ts_user  a on a.id  = t.user_id\n" +
                "                       WHERE\n" +
                "                           user_id = 111\n" +
                "                           AND t.is_delete = 0\n" +
                "                           AND t.add_time <= UNIX_TIMESTAMP()\n" +
                "                           AND t.status = 2\n" +
                "                           AND t.process_status != 8\n" +
                "                           AND assess.order_id is null");
        Select selectStatement = (Select) statement;
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
        System.out.println(tableList);

    }
}
