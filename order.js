import mysql from "mysql2/promise";
import wt from "worker_threads";

class Order {
    /*
    static async precheck(HOST, USER, PASSWORD, DATABASE) {
        let conn;
        try {
            conn = await mysql.createConnection({
                host: HOST,
                user: USER,
                password: PASSWORD,
                database: DATABASE,
                multipleStatements: false
            });
            await this.createtable(false, conn);
        } catch (error){
            if(error.code === 'ER_BAD_DB_ERROR'){
                conn = await mysql.createConnection({
                    host: HOST,
                    user: USER,
                    password: PASSWORD,
                    multipleStatements: false
                });
                await conn.execute('CREATE DATABASE ' + DATABASE);
                await conn.end();
                conn = await mysql.createConnection({
                    host: HOST,
                    user: USER,
                    password: PASSWORD,
                    database: DATABASE,
                    multipleStatements: false
                });
                await this.createtable(false, conn);
            } else {
                throw error;
            }
        }
        if (conn) {
            await conn.end();
        }
    }
    static async createtable(force, con) {
        if(force){
            await con.execute('DROP TABLE IF EXISTS lineitem FORCE');
        }
        let results = [];
        try {
            [results] = await con.execute(
                'CREATE TABLE lineitem (\n' +
                '   l_orderkey bigint(11) NOT NULL,\n' +
                '   l_partkey int(11) NOT NULL,\n' +
                '   l_suppkey int(11) NOT NULL,\n' +
                '   l_linenumber int(11) NOT NULL,\n' +
                '   l_quantity decimal(15,2) NOT NULL,\n' +
                '   l_extendedprice decimal(15,2) NOT NULL,\n' +
                '   l_discount decimal(15,2) NOT NULL,\n' +
                '   l_tax decimal(15,2) NOT NULL,\n' +
                '   l_returnflag char(1) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,\n' +
                '   l_linestatus char(1) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,\n' +
                '   l_shipdate date NOT NULL,\n' +
                '   l_commitdate date NOT NULL,\n' +
                '   l_receiptdate date NOT NULL,\n' +
                '   l_shipinstruct char(25) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,\n' +
                '   l_shipmode char(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,\n' +
                '   l_comment varchar(44) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,' +
                '   sort KEY (l_orderkey),\n' +
                '   shard key(l_orderkey),\n' +
                '   unique key(l_orderkey, l_linenumber) using hash,\n' +
                '   key(l_orderkey) using hash,\n' +
                '   key(l_partkey) using hash,\n' +
                '   key(l_suppkey) using hash\n' +
                ')');
            console.log("Created table lineitemjson")
            await this.createpipeline(true , con);
        } catch (err){
            if(err.code !== 'ER_TABLE_EXISTS_ERROR'){
                console.log(err)
                throw err;
            }
        }
        return results.affectedRows;
    };

    static async createpipeline(force, con) {
        if(force){
            await con.execute('DROP PIPELINE IF EXISTS tpch_100_lineitem');
        }
        //await con.execute('DELIMITER //');
        const [ results ] = await con.execute('CREATE PIPELINE tpch_100_lineitem\n' +
            'AS LOAD DATA S3 \'memsql-tpch-dataset/sf_100/lineitem/\'\n' +
            'config \'{"region":"us-east-1"}\'\n' +
            'SKIP DUPLICATE KEY ERRORS\n' +
            'INTO TABLE lineitem\n' +
            'FIELDS TERMINATED BY \'|\'\n' +
            'LINES TERMINATED BY \'|\\n\'');
        const [ results2 ] = await con.execute('START PIPELINE tpch_100_lineitem');
        return {"PipelineCreateRows": results.affectedRows, "PipelineRows": results2.affectedRows};
    };

    static async getbyorder(l_orderkey, conn) {
        const [rows] = await conn.query(
            'SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, \n' +
            'l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment \n' +
            'FROM lineitem WHERE l_orderkey = ? limit 1000',
            [l_orderkey]);
        return rows;
    };


    static async getbyorderline(l_orderkey, l_linenumber, conn) {
        const [rows] = await conn.query(
            'SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, \n' +
            'l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment \n' +
            'FROM lineitem WHERE l_orderkey = ? and l_linenumber = ? limit 1000',
            [l_orderkey, l_linenumber]);
        return rows;
    };

    static async getbypart(l_partkey, conn) {
        const [rows] = await conn.query(
            'SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, \n' +
            'l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment \n' +
            'FROM lineitem WHERE l_partkey = ? limit 1000',
            [l_partkey]);
        return rows;
    };

    static async getbysupplier(l_suppkey, conn) {
        const [rows] = await conn.query(
            'SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, \n' +
            'l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment \n' +
            'FROM lineitem WHERE l_suppkey = ? limit 1000',
            [l_suppkey]);
        return rows;
    };

    static async getnextorder(conn) {
        const [rows] = await conn.query('SELECT max(l_orderkey) + 1 as nextorder FROM lineitem');
        return rows;
    };


    static async insert(detail, conn) {
        const [results] = await conn.execute(
            'INSERT INTO lineitem ' +
            '(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, \n' +
            'l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) \n' +
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ',
            [ detail.l_orderkey, detail.l_partkey, detail.l_suppkey, detail.l_linenumber, detail.l_quantity, detail.l_extendedprice,
                detail.l_discount, detail.l_tax, detail.l_returnflag, detail.l_linestatus, detail.l_shipdate, detail.l_commitdate,
                detail.l_receiptdate, detail.l_shipinstruct, detail.l_shipmode, detail.l_comment]);
        return results.affectedRows;
    };
*/
    static async bullkInsert(detailArray, table, conn) {
        // console.log('bulk insert called.');
        const [results] = await conn.query(
            'INSERT INTO ' + table + ' ' +
            '(\n' +
            '  id, shop_domain, app_id, billing_address, \n' +
            '  browser_ip, buyer_accepts_marketing, \n' +
            '  cancel_reason, cancelled_at, cart_token, \n' +
            '  checkout_token, client_details_user_agent, \n' +
            '  closed_at, created_at, currency, \n' +
            '  current_subtotal, current_total, \n' +
            '  customer_id, customer_locale, email, \n' +
            '  financial_status, location_id, name, \n' +
            '  note, note_attribute, number, order_number, \n' +
            '  order_status_url, original_total_duties_set, \n' +
            '  payment_gateway_names, presentment_currency, \n' +
            '  processed_at, processing_method, \n' +
            '  referring_site, shipping_address, \n' +
            '  source_identifier, source_name, \n' +
            '  source_url, subtotal_price, subtotal_price_set, \n' +
            '  taxes_included, total_discounts, \n' +
            '  total_discounts_set, total_line_items_price, \n' +
            '  total_line_items_price_set, total_price, \n' +
            '  total_price_set, total_price_usd, \n' +
            '  total_shipping_price_set, total_tax, \n' +
            '  total_tax_set, total_tip_received, \n' +
            '  total_weight, updated_at\n' +
            ') ' +
            'VALUES ? ' +
            'on duplicate key \n' +
            'update \n' +
            '  id = \n' +
            'VALUES \n' +
            '  (id), \n' +
            '  shop_domain = \n' +
            'VALUES \n' +
            '  (shop_domain), \n' +
            '  app_id = \n' +
            'VALUES \n' +
            '  (app_id), \n' +
            '  billing_address = \n' +
            'VALUES \n' +
            '  (billing_address), \n' +
            '  browser_ip = \n' +
            'VALUES \n' +
            '  (browser_ip), \n' +
            '  buyer_accepts_marketing = \n' +
            'VALUES \n' +
            '  (buyer_accepts_marketing), \n' +
            '  cancel_reason = \n' +
            'VALUES \n' +
            '  (cancel_reason), \n' +
            '  cancelled_at = \n' +
            'VALUES \n' +
            '  (cancelled_at), \n' +
            '  cart_token = \n' +
            'VALUES \n' +
            '  (cart_token), \n' +
            '  checkout_token = \n' +
            'VALUES \n' +
            '  (checkout_token), \n' +
            '  client_details_user_agent = \n' +
            'VALUES \n' +
            '  (client_details_user_agent), \n' +
            '  closed_at = \n' +
            'VALUES \n' +
            '  (closed_at), \n' +
            '  created_at = \n' +
            'VALUES \n' +
            '  (created_at), \n' +
            '  currency = \n' +
            'VALUES \n' +
            '  (currency), \n' +
            '  current_subtotal = \n' +
            'VALUES \n' +
            '  (current_subtotal), \n' +
            '  current_total = \n' +
            'VALUES \n' +
            '  (current_total), \n' +
            '  customer_id = \n' +
            'VALUES \n' +
            '  (customer_id), \n' +
            '  customer_locale = \n' +
            'VALUES \n' +
            '  (customer_locale), \n' +
            '  email = \n' +
            'VALUES \n' +
            '  (email), \n' +
            '  financial_status = \n' +
            'VALUES \n' +
            '  (financial_status), \n' +
            '  landing_site_base_url = \n' +
            'VALUES \n' +
            '  (landing_site_base_url), \n' +
            '  location_id = \n' +
            'VALUES \n' +
            '  (location_id), \n' +
            '  name = \n' +
            'VALUES \n' +
            '  (name), \n' +
            '  note = \n' +
            'VALUES \n' +
            '  (note), \n' +
            '  note_attribute = \n' +
            'VALUES \n' +
            '  (note_attribute), \n' +
            '  number = \n' +
            'VALUES \n' +
            '  (number), \n' +
            '  order_number = \n' +
            'VALUES \n' +
            '  (order_number), \n' +
            '  order_status_url = \n' +
            'VALUES \n' +
            '  (order_status_url), \n' +
            '  original_total_duties_set = \n' +
            'VALUES \n' +
            '  (original_total_duties_set), \n' +
            '  payment_gateway_names = \n' +
            'VALUES \n' +
            '  (payment_gateway_names), \n' +
            '  presentment_currency = \n' +
            'VALUES \n' +
            '  (presentment_currency), \n' +
            '  processed_at = \n' +
            'VALUES \n' +
            '  (processed_at), \n' +
            '  processing_method = \n' +
            'VALUES \n' +
            '  (processing_method), \n' +
            '  referring_site = \n' +
            'VALUES \n' +
            '  (referring_site), \n' +
            '  shipping_address = \n' +
            'VALUES \n' +
            '  (shipping_address), \n' +
            '  source_identifier = \n' +
            'VALUES \n' +
            '  (source_identifier), \n' +
            '  source_name = \n' +
            'VALUES \n' +
            '  (source_name), \n' +
            '  source_url = \n' +
            'VALUES \n' +
            '  (source_url), \n' +
            '  subtotal_price = \n' +
            'VALUES \n' +
            '  (subtotal_price), \n' +
            '  subtotal_price_set = \n' +
            'VALUES \n' +
            '  (subtotal_price_set), \n' +
            '  taxes_included = \n' +
            'VALUES \n' +
            '  (taxes_included), \n' +
            '  total_discounts = \n' +
            'VALUES \n' +
            '  (total_discounts), \n' +
            '  total_discounts_set = \n' +
            'VALUES \n' +
            '  (total_discounts_set), \n' +
            '  total_line_items_price = \n' +
            'VALUES \n' +
            '  (total_line_items_price), \n' +
            '  total_line_items_price_set = \n' +
            'VALUES \n' +
            '  (total_line_items_price_set), \n' +
            '  total_price = \n' +
            'VALUES \n' +
            '  (total_price), \n' +
            '  total_price_set = \n' +
            'VALUES \n' +
            '  (total_price_set), \n' +
            '  total_price_usd = \n' +
            'VALUES \n' +
            '  (total_price_usd), \n' +
            '  total_shipping_price_set = \n' +
            'VALUES \n' +
            '  (total_shipping_price_set), \n' +
            '  total_tax = \n' +
            'VALUES \n' +
            '  (total_tax), \n' +
            '  total_tax_set = \n' +
            'VALUES \n' +
            '  (total_tax_set), \n' +
            '  total_tip_received = \n' +
            'VALUES \n' +
            '  (total_tip_received), \n' +
            '  total_weight = \n' +
            'VALUES \n' +
            '  (total_weight), \n' +
            '  updated_at = \n' +
            'VALUES \n' +
            '  (updated_at)'
            , [detailArray]);
        return results.affectedRows;
    };
/*
    static async delete(l_orderkey, l_linenumber, conn) {
        const [results] = await conn.execute(
            'DELETE FROM lineitem WHERE l_orderkey = ? and l_linenumber = ?',
            [l_orderkey, l_linenumber]);
        return results.affectedRows;
    };

    static async deleteorder(l_orderkey, conn) {
        const [results] = await conn.execute(
            'DELETE FROM lineitem WHERE l_orderkey = ?',
            [l_orderkey]);
        return results.affectedRows;
    };

    static async update(detail, conn) {
        const [results] = await conn.execute(
            'Update lineitem set l_comment = ? where l_orderkey = ? and l_linenumber = ?',
            [detail.l_comment, detail.l_orderkey, detail.l_linenumber]);
        return results.affectedRows;
    };
*/
}

export default Order;
