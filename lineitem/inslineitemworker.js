'use strict'
import wt from 'worker_threads'
import mysql from "mysql2/promise";
import lineitem1 from "./lineitemjson.js";
import lineitem2 from "./lineitem.js";


import casual from "casual";

let con;
try {
    con = await mysql.createConnection({
        host:       wt.workerData.host,
        user:       wt.workerData.user,
        password:   wt.workerData.password,
        database:   wt.workerData.database,
        connectTimeout: 60000
    });
    let lineitem = (wt.workerData.jsontest)?lineitem1:lineitem2;
    let rows = 0;
    try {
        let details = [];
        let linenumber = 0;
        let sendsize = wt.workerData.sendsize;
        for(let i=0;i<wt.workerData.batchsize;i++) {
            if (linenumber < 1) {
                linenumber = casual.integer(1, wt.workerData.maxlines);
                if(i + linenumber > wt.workerData.batchsize){  // enforce all batches include 1.
                    linenumber = wt.workerData.batchsize - i;
                }
            }
            if(wt.workerData.jsontest){
                details.push( [
                    i + wt.workerData.startval,
                    linenumber,
                    JSON.stringify({
                        "l_comment":casual.description,
                        "l_commitdate":casual.date("YYYY-MM-DD"),
                        "l_discount":casual.double(0.0, 10.0),
                        "l_extendedprice":casual.double(wt.workerData.minprice, wt.workerData.maxprice),
                        "l_linenumber":linenumber,
                        "l_linestatus":casual.letter,
                        "l_orderkey":i + wt.workerData.startval,
                        "l_partkey":casual.integer(1, wt.workerData.maxpartkey),
                        "l_quantity":casual.integer(1,100),
                        "l_receiptdate":casual.date("YYYY-MM-DD"),
                        "l_returnflag":casual.letter,
                        "l_shipdate":casual.date("YYYY-MM-DD"),
                        "l_shipinstruct":casual.string,
                        "l_shipmode":casual.short_description,
                        "l_suppkey":casual.integer(1, wt.workerData.maxsupkey),
                        "l_tax":casual.double(0.0,10.0)})
                ]);
            }else {
                details.push( [i + wt.workerData.startval,
                    casual.integer(1, wt.workerData.maxpartkey),
                    casual.integer(1, wt.workerData.maxsupkey),
                    linenumber,
                    casual.integer(1, 100),
                    casual.double(wt.workerData.minprice, wt.workerData.maxprice),
                    casual.double(0.0, 10.0),
                    casual.double(0.0, 10.0),
                    casual.letter,
                    casual.letter,
                    casual.date("YYYY-MM-DD"),
                    casual.date("YYYY-MM-DD"),
                    casual.date("YYYY-MM-DD"),
                    casual.string,
                    casual.short_description,
                    casual.description
                ]);
            }
            linenumber--;
            if(details.length >= sendsize){
                rows = rows + await lineitem.bullkInsert( details, con);
                wt.parentPort.postMessage({
                    msg: `Inserted Batch rows: ${details.length}`,
                    thread: wt.workerData.thread});
                details = [];
            }
        }
        if(details.length > 0){
            rows = rows + await lineitem.bullkInsert( details, con);
            wt.parentPort.postMessage({
                msg: `Inserted Batch rows: ${details.length}`,
                thread: wt.workerData.thread});
        }
        wt.parentPort.postMessage({
            msg: `Inserted all rows: ${rows}`,
            thread: wt.workerData.thread})
    }catch (err){
        if(err.code === 'ER_DUP_ENTRY'){
            wt.parentPort.postMessage({ msg: 'Duplicate Key', thread: wt.workerData.thread, dbtime: 0 })
        }else {
            wt.parentPort.postMessage({ msg: err, thread: wt.workerData.thread, dbtime: 0 })
        }
    }
}catch (err){
    wt.parentPort.postMessage({ msg: err, thread: wt.workerData.thread, dbtime: 0 })
} finally {
    if (con) {
        await con.end();
    }
}
