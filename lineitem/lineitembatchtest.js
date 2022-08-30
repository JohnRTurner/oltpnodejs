import lineitem1 from "./lineitemjson.js";
import lineitem2 from "./lineitem.js";
import status from "../data/status.js";

import mysql from "mysql2/promise";
import Worker from "worker_threads";

export async function lineitembatchtest(HOST, USER, PASSWORD, DATABASE, BATCHSIZE, SENDSIZE, THREADS,
                                            MAXLINES, MAXPARTKEY, MAXSUPPKEY, MINPRICE, MAXPRICE, JSONTEST) {
    if(status.getStatus() !== "Idle"){
        console.log(`Will not start as server status is not Idle, but is ${status.getStatus()} !`)
        return false;
    }
    status.updateStatus("Starting");

    let lineitem = (JSONTEST)?lineitem1: lineitem2;
    await lineitem.precheck(HOST, USER, PASSWORD, DATABASE);
    const con = await mysql.createPool({
        host: HOST,
        user: USER,
        password: PASSWORD,
        database: DATABASE,
        connectionLimit: 1
    })
    if (!con) {
        console.error("No Pool Found!")
        process.exit(1);
    }
    let STARTVAL = (await lineitem.getnextorder(con))[0].nextorder;
    if (!STARTVAL) {
        console.error("No order found from lineitem using 1" )
        //process.exit(1);
        STARTVAL = 1
    }

    let threads = [];
    const totstart = Date.now();
    for (let i = 1; i <= THREADS; i++) {
        threads[i] = new Promise((resolve, reject) => {
            const start = Date.now();
            new Worker.Worker( './lineitem/inslineitemworker.js', {
                workerData: {
                    host: HOST,
                    user: USER,
                    password: PASSWORD,
                    database: DATABASE,
                    batchsize: BATCHSIZE,
                    sendsize: SENDSIZE,
                    maxlines: MAXLINES,
                    startval: STARTVAL + ((i - 1) * BATCHSIZE),
                    minprice: MINPRICE,
                    maxprice: MAXPRICE,
                    maxpartkey: MAXPARTKEY,
                    maxsupkey: MAXSUPPKEY,
                    thread: i,
                    jsontest: JSONTEST
                }
            }).on('message', (msg) => {
                status.pushUpdate({'Thread': msg.thread, 'Time': (Date.now() - start) / 1000.0, 'Msg': msg.msg })
            }).on('error', (err) => {
                status.pushUpdate({'Thread': i, 'Time': (Date.now() - start) / 1000.0, 'Msg': `Error: ${err}` })
                reject();
            }).on('exit', (code) => {
                if (code !== 0) {
                    // console.log(`Thread ${i} has completed with code: ${code} in ${(Date.now() - start) / 1000.0} seconds.`)
                    status.pushUpdate({'Thread': i, 'Time': (Date.now() - start) / 1000.0, 'Msg': `Exit Code: ${code}` })
                }
                resolve();
            })
        });
        await status.delay(20); //Avoid spiking connection creation
    }
    status.updateStatus("Processing");
    await Promise.allSettled(threads).then(() => {
        const tm = (Date.now() - totstart) / 1000.0;
        status.pushUpdate( { 'Threads': THREADS, 'Time': tm, 'Msg': `'InsertRows': ${THREADS * BATCHSIZE}, 'RowsPerSecond': ${(THREADS * BATCHSIZE) / tm},   'MillisecondsPerRow': ${(tm * 1000) / (THREADS * BATCHSIZE)} `});
    });
    status.updateStatus("Idle");
    return true;
}
