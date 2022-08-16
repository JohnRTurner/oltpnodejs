import mysql from "mysql2/promise";

class Lineitem {
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

    static async bullkInsert(detailArray, conn) {
        const [results] = await conn.query(
            'INSERT INTO lineitem ' +
            '(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, \n' +
            'l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) \n' +
            'VALUES ?', [detailArray]);
        return results.affectedRows;
    };

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

}

export default Lineitem;
