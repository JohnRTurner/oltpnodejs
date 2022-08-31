'use strict'
import wt from 'worker_threads'
import mysql from "mysql2/promise";
import lineitem1 from "./lineitemjson.js";
import lineitem2 from "./lineitem.js";
// import {datetime as DateTime, timedelta as TimeDelta} from "datetime";

import casual from "casual";
//casual.seed()

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



        let mxsz = Number.MAX_SAFE_INTEGER - 25
        let pricerange100 = (wt.workerData.maxprice - wt.workerData.minprice) * 100
        let startdt = new Date() - 30

        let randlistsize = 1001
        let rndlst = []
        for(let i = 0; i < randlistsize; i++){
            rndlst[i] = casual.short_description
        }


        for(let i=0;i<wt.workerData.batchsize;i++) {
            let rndcnt = casual.integer(1, mxsz)
            let pkey = (rndcnt % wt.workerData.maxpartkey) + 1
            let skey = (rndcnt % wt.workerData.maxsupkey) + 1
            let disc =Math.round((rndcnt % 101)) / 10.0
            let tax = Math.round((rndcnt % 10001)/ 100) / 10.0
            let qty = Math.round((rndcnt % 1000000) / 10000) + 1
            let eprice = wt.workerData.minprice + (Math.round(rndcnt % pricerange100) / 100.0)
            let linestatus = String.fromCharCode(97 /*ascii A */ + (rndcnt % 26))
            let retflag = String.fromCharCode(97 /*ascii A */ + (pkey % 26))
            let shipdate = startdt + (rndcnt % 23)
            let recdate = shipdate + ((rndcnt%4) + 1)
            let comdate = shipdate + ((rndcnt%5) + 3)
            let shipmode = rndlst[(rndcnt % randlistsize)]
            let shipinstr = rndlst[((rndcnt + 1) % randlistsize)] + " " + rndlst[((rndcnt + 5) % randlistsize)] + " " + rndlst[((rndcnt + 3) % randlistsize)]
            let comment = rndlst[((rndcnt + 2) % randlistsize)] + " " +  rndlst[((rndcnt + 4) % randlistsize)] + rndlst[((rndcnt + 6) % randlistsize)] +
                rndlst[((rndcnt + 7) % randlistsize)] + " " +  rndlst[((rndcnt + 9) % randlistsize)] + rndlst[((rndcnt + 8) % randlistsize)]

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
                        "l_comment":comment,
                        "l_commitdate":comdate,
                        "l_discount":disc,
                        "l_extendedprice":eprice,
                        "l_linenumber":linenumber,
                        "l_linestatus":linestatus,
                        "l_orderkey":i + wt.workerData.startval,
                        "l_partkey":pkey,
                        "l_quantity":qty,
                        "l_receiptdate":recdate,
                        "l_returnflag":retflag,
                        "l_shipdate":shipdate,
                        "l_shipinstruct":shipinstr,
                        "l_shipmode":shipmode,
                        "l_suppkey":skey,
                        "l_tax":tax})
                ]);
            }else {
                details.push( [i + wt.workerData.startval, pkey, skey, linenumber, qty, eprice, disc, tax, retflag,
                    linestatus, comdate, recdate, shipdate, shipinstr, shipmode, comment]);
            }
            linenumber--;
            if(details.length >= sendsize){
                rows = rows + await lineitem.bullkInsert( details, con);
                wt.parentPort.postMessage({
                    msg: `Inserted Batch rows: ${details.length}`,
                    thread: wt.workerData.thread});
                details = [];
            }
            /*
            if( i == 0){
                console.log(details.at(0))
            }
            */
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
