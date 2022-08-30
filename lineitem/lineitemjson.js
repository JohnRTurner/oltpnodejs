import mysql from "mysql2/promise";

class Lineitemjson {
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
            await con.execute('DROP TABLE IF EXISTS lineitemjson FORCE');
        }
        let results = [];
        try {
            [results] = await con.execute(
                'CREATE TABLE lineitemjson (\n' +
                '   detail JSON NOT NULL,\n' +
                '   l_orderkey bigint(11) NOT NULL,\n' +
                '   l_linenumber bigint(11) NOT NULL,\n' +
                '   l_partkey as detail::%l_partkey persisted bigint(11) NOT NULL,\n' +
                '   l_suppkey as detail::%l_suppkey persisted bigint(11) NOT NULL,\n' +
                '   l_extendedprice as detail::%l_extendedprice persisted decimal(15,2) NOT NULL,\n' +
                '   detailstring as json_pretty(detail) persisted text NOT NULL,\n' +
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
            await con.execute('DROP PROCEDURE IF EXISTS ins_lineitemjson');
            await con.execute('DROP PIPELINE IF EXISTS tpch_100_lineitemjson');
        }
        //await con.execute('DELIMITER //');
        const results = await con.execute(
                'CREATE OR REPLACE PROCEDURE ins_lineitemjson(batch QUERY( ' +
                '                  l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber BIGINT, ' +
                '                  l_quantity DECIMAL, l_extendedprice DECIMAL, l_discount DECIMAL, l_tax DECIMAL, ' +
                '                  l_returnflag char, l_linestatus char, l_shipdate date, l_commitdate date, ' +
                '                  l_receiptdate date, l_shipinstruct text, l_shipmode text, l_comment text)) ' +
                'AS BEGIN  ' +
                '    insert into lineitemjson(l_orderkey, l_linenumber, detail) ' +
                '      select batch.l_orderkey, batch.l_linenumber, TO_JSON(batch.*) as detail from batch; ' +
                'END');
        const [ results2 ] = await con.execute('CREATE PIPELINE tpch_100_lineitemjson\n' +
            'AS LOAD DATA S3 \'memsql-tpch-dataset/sf_100/lineitem/\'\n' +
            'config \'{"region":"us-east-1"}\'\n' +
            'INTO PROCEDURE ins_lineitemjson\n' +
            'FIELDS TERMINATED BY \'|\'\n' +
            'LINES TERMINATED BY \'|\\n\'');
        const [ results3 ] = await con.execute('START PIPELINE tpch_100_lineitemjson');
        return {"procedureRows": results.affectedRows, "PipelineCreateRows": results2.affectedRows, "PipelineRows": results3.affectedRows};
    };



    static async getbyordernojson(l_orderkey, conn) {
        const [rows] = await conn.query(
            'SELECT l_orderkey, l_linenumber,l_partkey,l_suppkey,l_extendedprice FROM lineitemjson WHERE l_orderkey = ? limit 1000',
            [l_orderkey]);
        return rows;
    };


    static async getbyorder(l_orderkey, conn) {
        const [rows] = await conn.query(
            'SELECT detail FROM lineitemjson WHERE l_orderkey = ? limit 1000',
            [l_orderkey]);
        return rows;
    };

    static async getbyorderline(l_orderkey, l_linenumber, conn) {
        const [rows] = await conn.query(
            'SELECT detail FROM lineitemjson WHERE l_orderkey = ? and l_linenumber = ? limit 1000',
            [l_orderkey, l_linenumber]);
        return rows;
    };

    static async getbypart(l_partkey, conn) {
        const [rows] = await conn.query(
            'SELECT detail FROM lineitemjson WHERE l_partkey = ? limit 1000',
            [l_partkey]);
        return rows;
    };

    static async getbysupplier(l_suppkey, conn) {
        const [rows] = await conn.query(
            'SELECT detail FROM lineitemjson WHERE l_suppkey = ? limit 1000',
            [l_suppkey]);
        return rows;
    };

    static async getnextorder(conn) {
        const [rows] = await conn.query('SELECT max(l_orderkey) + 1 as nextorder FROM lineitemjson');
        return rows;
    };


    static async insert(detail, conn) {
        const [results] = await conn.execute(
            'INSERT INTO lineitemjson (l_orderkey, l_linenumber, detail) VALUES (?, ?, ?)',
            [detail.l_orderkey, detail.l_linenumber, JSON.stringify(detail) ]);
        return results.affectedRows;
    };

    static async bullkInsert(detailArray, conn) {
        const [results] = await conn.query(
            'INSERT INTO lineitemjson (l_orderkey, l_linenumber, detail) VALUES ?', [detailArray]);
        return results.affectedRows;
    };

    static async delete(l_orderkey, l_linenumber, conn) {
        const [results] = await conn.execute(
            'DELETE FROM lineitemjson WHERE l_orderkey = ? and l_linenumber = ?',
            [l_orderkey, l_linenumber]);
        return results.affectedRows;
    };

    static async deleteorder(l_orderkey, conn) {
        const [results] = await conn.execute(
            'DELETE FROM lineitemjson WHERE l_orderkey = ?',
            [l_orderkey]);
        return results.affectedRows;
    };

    static async update(detail, conn) {
        const [results] = await conn.execute(
            'Update lineitemjson set detail = ? where l_orderkey = ? and l_linenumber = ?',
            [JSON.stringify(detail), detail.l_orderkey, detail.l_linenumber]);
        return results.affectedRows;
    };

}

export default Lineitemjson;
