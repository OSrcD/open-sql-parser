package com.alibaba.druid.sql.mysql.bvt;

import java.util.List;

import junit.framework.TestCase;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.parser.SQLStatementParser;

public class SQLSelectTest extends TestCase {
    public void test_select() throws Exception { // 已测试完
        String sql = "SELECT ALL FID FROM T1;SELECT DISTINCT FID FROM T1;SELECT DISTINCTROW FID FROM T1;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        output(stmtList);
    }

    public void test_select_1() throws Exception {
        String sql =
                "SELECT HIGH_PRIORITY STRAIGHT_JOIN SQL_SMALL_RESULT SQL_BIG_RESULT SQL_BUFFER_RESULT FID FROM T1;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        output(stmtList);
    }

    public void test_select_2() throws Exception {
        String sql = "SELECT SQL_CACHE FID FROM T1;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        output(stmtList);
    }

    public void test_select_3() throws Exception {
        String sql = "SELECT SQL_NO_CACHE FID FROM T1;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        output(stmtList);
    }

    public void test_select_4() throws Exception {
        String sql = "SELECT SQL_CALC_FOUND_ROWS FID FROM T1;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        output(stmtList);
    }

    public void test_select_5() throws Exception {
        String sql = "SELECT 1 + 1;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        output(stmtList);
    }

    public void test_select_6() throws Exception {
        String sql = " select  uuid()              as pk_id\n" +
                "        ,cust_id            as cust_id\n" +
                "        ,z.uid              as uid\n" +
                "        ,contact_no         as contact_no\n" +
                "        ,cnty_cd            as cnty_cd\n" +
                "        ,area_no            as area_no\n" +
                "        ,ext_no             as ext_no\n" +
                "        ,contact_typ        as contact_typ\n" +
                "        ,chnl_id            as chnl_id\n" +
                "        ,sc_ind             as sc_ind\n" +
                "        ,status             as status\n" +
                "        ,priority           as priority\n" +
                "        ,contact_no_weight  as contact_no_weight\n" +
                "        ,t3.contact_type    as is_bind\n" +
                "        ,t4.contact_type               as bind_dt\n" +
                "        ,null               as is_flt_delay_phone\n" +
                "        ,interact_score     as interact_score\n" +
                " from    (\n" +
                "        select  cust_id\n" +
                "                ,uid\n" +
                "                ,contact_no\n" +
                "                ,cnty_cd\n" +
                "                ,area_no\n" +
                "                ,ext_no\n" +
                "                ,contact_typ\n" +
                "                ,chnl_id\n" +
                "                ,sc_ind\n" +
                "                ,status\n" +
                "                ,priority\n" +
                "                ,contact_no_weight\n" +
                "                ,interact_score\n" +
                "                ,row_number() over(partition by cust_id, contact_typ, contact_no, contact_no_weight, priority) rn\n" +
                "        from    (\n" +
                "                        select  t.one_id                                as cust_id\n" +
                "                                ,t.uid                                  as uid\n" +
                "                                ,case   when t1.telephone is not null and t2.phone_type = 'mobile' then t1.telephone\n" +
                "                                        else t2.contact_no\n" +
                "                                end                                     as contact_no\n" +
                "                                ,t2.country_code                        as cnty_cd\n" +
                "                                ,t2.area_no                             as area_no\n" +
                "                                ,t2.ext_no                              as ext_no\n" +
                "                                ,case   when t1.telephone is not null and t2.phone_type = 'mobile' then 'mobile'\n" +
                "                                        else t2.phone_type\n" +
                "                                end                                     as contact_typ\n" +
                "                                ,case   when t1.telephone is not null and t2.phone_type = 'mobile' then t1.channle_id\n" +
                "                                        else t2.channel_id\n" +
                "                                end                                     as chnl_id\n" +
                "                                ,t2.associate_type                      as sc_ind\n" +
                "                                ,t2.status                              as status\n" +
                "                                ,t2.priority                            as priority\n" +
                "                                ,case   when t1.telephone is not null and t2.phone_type = 'mobile' then t1.weight_level\n" +
                "                                end                                     as contact_no_weight\n" +
                "                                ,t1.interact_score                      as interact_score\n" +
                "                        from\n" +
                "                                (\n" +
                "                                        select  one_id\n" +
                "                                                ,data_value as uid\n" +
                "                                        from    czcdm.mid_cbd_oneid_certificate\n" +
                "                                        where   ds = '${bizdate}'\n" +
                "                                        and     data_label = 'uid'\n" +
                "\n" +
                "                                ) t\n" +
                "                        left outer join\n" +
                "                                (\n" +
                "                                        select  one_id\n" +
                "                                                ,uid\n" +
                "                                                ,telephone\n" +
                "                                                ,weight_level\n" +
                "                                                ,channle_id\n" +
                "                                                ,interact_score\n" +
                "                                        from\n" +
                "                                                (\n" +
                "                                                        select  one_id\n" +
                "                                                                ,uid\n" +
                "                                                                ,telephone\n" +
                "                                                                ,weight_level\n" +
                "                                                                ,channle_id\n" +
                "                                                                ,interact_score\n" +
                "                                                                ,row_number() over(partition by one_id, telephone order by op_date desc) rn\n" +
                "                                                        from    czods.s_cbd_oneid_phone\n" +
                "                                                        where   ds = '${bizdate}'\n" +
                "\n" +
                "                                                ) v\n" +
                "                                        where v.rn = 1\n" +
                "                                ) t1\n" +
                "                        on      t1.one_id = t.one_id\n" +
                "                        left outer join\n" +
                "                                (\n" +
                "                                        select  uid\n" +
                "                                                ,contact_no\n" +
                "                                                ,country_code\n" +
                "                                                ,area_no\n" +
                "                                                ,ext_no\n" +
                "                                                ,phone_type\n" +
                "                                                ,channel_id\n" +
                "                                                ,associate_type\n" +
                "                                                ,status\n" +
                "                                                ,priority\n" +
                "                                        from    (\n" +
                "                                                        select  uid\n" +
                "                                                                ,contact_no         as contact_no\n" +
                "                                                                ,country_code     as country_code\n" +
                "                                                                ,area_no               as area_no\n" +
                "                                                                ,ext_no                 as ext_no\n" +
                "                                                                ,phone_type         as phone_type\n" +
                "                                                                ,channel_id         as channel_id\n" +
                "                                                                ,associate_type as associate_type\n" +
                "                                                                ,status                 as status\n" +
                "                                                                ,priority             as priority\n" +
                "                                                                ,row_number() over(partition by uid, phone_type, contact_no, priority order by op_date desc) rn\n" +
                "                                                        from    czods.s_svcdb_svc_pc_contact\n" +
                "                                                        where   ds = '${bizdate}'\n" +
                "                                                        and     nvl(contact_no,'') != ''\n" +
                "\n" +
                "                                                ) v\n" +
                "                                        where   v.rn = 1\n" +
                "                                ) t2\n" +
                "                        on      t2.uid = t.uid\n" +
                "                ) w\n" +
                "        ) z\n" +
                " left join\n" +
                "        (\n" +
                "            select  uid, contact_type  as contact_type\n" +
                "            from    (\n" +
                "                        select  uid,contact_type,status,row_number() over(partition by uid,contact_type order by op_date desc ) as rn\n" +
                "                        from    czods.s_svcdb_svc_pc_web_contact_info\n" +
                "                        where   ds = max_pt('czods.s_svcdb_svc_pc_web_contact_info')\n" +
                "                    )   t3\n" +
                "            where   t3.rn = 1\n" +
                "            and     t3.status = 'Y'\n" +
                "            group by uid\n" +
                "        )     t3\n" +
                " on      z.uid = t3.uid\n" +
                " left join\n" +
                "         (\n" +
                "             select  uid, contact_type  as contact_type\n" +
                "             from    (\n" +
                "                         select  uid,contact_type,status,row_number() over(partition by uid,contact_type order by op_date desc ) as rn\n" +
                "                         from    czods.s_svcdb_svc_pc_web_contact_info\n" +
                "                         where   ds = max_pt('czods.s_svcdb_svc_pc_web_contact_info')\n" +
                "                     )   t3\n" +
                "             where   t3.rn = 1\n" +
                "             and     t3.status = 'Y'\n" +
                "             group by uid\n" +
                "         )     t4\n" +
                "  on      z.uid = t4.uid\n" +
                " where   z.rn = 1;";


        sql = " select  uuid()              as pk_id\n" +
                "        ,cust_id            as cust_id\n" +
                "        ,z.uid              as uid\n" +
                "        ,contact_no         as contact_no\n" +
                "        ,cnty_cd            as cnty_cd\n" +
                "        ,area_no            as area_no\n" +
                "        ,ext_no             as ext_no\n" +
                "        ,contact_typ        as contact_typ\n" +
                "        ,chnl_id            as chnl_id\n" +
                "        ,sc_ind             as sc_ind\n" +
                "        ,status             as status\n" +
                "        ,priority           as priority\n" +
                "        ,contact_no_weight  as contact_no_weight\n" +
                "        ,t3.contact_type    as is_bind\n" +
                "        ,t4.contact_type               as bind_dt\n" +
                "        ,null               as is_flt_delay_phone\n" +
                "        ,interact_score     as interact_score\n" +
                " from    (\n" +
                "        select  cust_id\n" +
                "                ,uid\n" +
                "                ,contact_no\n" +
                "                ,cnty_cd\n" +
                "                ,area_no\n" +
                "                ,ext_no\n" +
                "                ,contact_typ\n" +
                "                ,chnl_id\n" +
                "                ,sc_ind\n" +
                "                ,status\n" +
                "                ,priority\n" +
                "                ,contact_no_weight\n" +
                "                ,interact_score\n" +
                "        from    (\n" +
                "                        select  t.one_id                                as cust_id\n" +
                "                                ,t.uid                                  as uid\n" +
                "                                ,case   when t1.telephone is not null and t2.phone_type = 'mobile' then t1.telephone\n" +
                "                                        else t2.contact_no\n" +
                "                                end                                     as contact_no\n" +
                "                                ,t2.country_code                        as cnty_cd\n" +
                "                                ,t2.area_no                             as area_no\n" +
                "                                ,t2.ext_no                              as ext_no\n" +
                "                                ,case   when t1.telephone is not null and t2.phone_type = 'mobile' then 'mobile'\n" +
                "                                        else t2.phone_type\n" +
                "                                end                                     as contact_typ\n" +
                "                                ,case   when t1.telephone is not null and t2.phone_type = 'mobile' then t1.channle_id\n" +
                "                                        else t2.channel_id\n" +
                "                                end                                     as chnl_id\n" +
                "                                ,t2.associate_type                      as sc_ind\n" +
                "                                ,t2.status                              as status\n" +
                "                                ,t2.priority                            as priority\n" +
                "                                ,case   when t1.telephone is not null and t2.phone_type = 'mobile' then t1.weight_level\n" +
                "                                end                                     as contact_no_weight\n" +
                "                                ,t1.interact_score                      as interact_score\n" +
                "                        from\n" +
                "                                (\n" +
                "                                        select  one_id\n" +
                "                                                ,data_value as uid\n" +
                "                                        from    czcdm.mid_cbd_oneid_certificate\n" +
                "                                        where   ds = '${bizdate}'\n" +
                "                                        and     data_label = 'uid'\n" +
                "\n" +
                "                                ) t\n" +
                "                        left outer join\n" +
                "                                (\n" +
                "                                        select  one_id\n" +
                "                                                ,uid\n" +
                "                                                ,telephone\n" +
                "                                                ,weight_level\n" +
                "                                                ,channle_id\n" +
                "                                                ,interact_score\n" +
                "                                        from\n" +
                "                                                (\n" +
                "                                                        select  one_id\n" +
                "                                                                ,uid\n" +
                "                                                                ,telephone\n" +
                "                                                                ,weight_level\n" +
                "                                                                ,channle_id\n" +
                "                                                                ,interact_score\n" +
                "                                                        from    czods.s_cbd_oneid_phone\n" +
                "                                                        where   ds = '${bizdate}'\n" +
                "\n" +
                "                                                ) v\n" +
                "                                        where v.rn = 1\n" +
                "                                ) t1\n" +
                "                        on      t1.one_id = t.one_id\n" +
                "                        left outer join\n" +
                "                                (\n" +
                "                                        select  uid\n" +
                "                                                ,contact_no\n" +
                "                                                ,country_code\n" +
                "                                                ,area_no\n" +
                "                                                ,ext_no\n" +
                "                                                ,phone_type\n" +
                "                                                ,channel_id\n" +
                "                                                ,associate_type\n" +
                "                                                ,status\n" +
                "                                                ,priority\n" +
                "                                        from    (\n" +
                "                                                        select  uid\n" +
                "                                                                ,contact_no         as contact_no\n" +
                "                                                                ,country_code     as country_code\n" +
                "                                                                ,area_no               as area_no\n" +
                "                                                                ,ext_no                 as ext_no\n" +
                "                                                                ,phone_type         as phone_type\n" +
                "                                                                ,channel_id         as channel_id\n" +
                "                                                                ,associate_type as associate_type\n" +
                "                                                                ,status                 as status\n" +
                "                                                                ,priority             as priority\n" +
                "                                                        from    czods.s_svcdb_svc_pc_contact\n" +
                "                                                        where   ds = '${bizdate}'\n" +
                "                                                        and     nvl(contact_no,'') != ''\n" +
                "\n" +
                "                                                ) v\n" +
                "                                        where   v.rn = 1\n" +
                "                                ) t2\n" +
                "                        on      t2.uid = t.uid\n" +
                "                ) w\n" +
                "        ) z\n" +
                " left join\n" +
                "        (\n" +
                "            select  uid, contact_type  as contact_type\n" +
                "            from    (\n" +
                "                        select  uid,contact_type,status\n" +
                "                        from    czods.s_svcdb_svc_pc_web_contact_info\n" +
                "                        where   ds = max_pt('czods.s_svcdb_svc_pc_web_contact_info')\n" +
                "                    )   t3\n" +
                "            where   t3.rn = 1\n" +
                "            and     t3.status = 'Y'\n" +
                "            group by uid\n" +
                "        )     t3\n" +
                " on      z.uid = t3.uid\n" +
                " left join\n" +
                "         (\n" +
                "             select  uid, contact_type  as contact_type\n" +
                "             from    (\n" +
                "                         select  uid,contact_type,status\n" +
                "                         from    czods.s_svcdb_svc_pc_web_contact_info\n" +
                "                         where   ds = max_pt('czods.s_svcdb_svc_pc_web_contact_info')\n" +
                "                     )   t3\n" +
                "             where   t3.rn = 1\n" +
                "             and     t3.status = 'Y'\n" +
                "             group by uid\n" +
                "         )     t4\n" +
                "  on      z.uid = t4.uid\n" +
                " where   z.rn = 1;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        output(stmtList);
    }

    public void test_select_join() throws Exception {
        String sql = "SELECT CONCAT(last_name,', ',first_name) AS full_name FROM mytable ORDER BY full_name;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        output(stmtList);
    }



    private void output(List<SQLStatement> stmtList) {
        for (SQLStatement stmt : stmtList) {
            stmt.accept(new MySqlOutputVisitor(System.out)); // 访问 SQLSelectStatment
            System.out.println(";"); // 表语法的;结束
            System.out.println(); // 换行
        }
    }
}
