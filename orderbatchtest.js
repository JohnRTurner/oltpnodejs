import mysql from "mysql2/promise";
import Worker from "worker_threads";

function delay(time) {
    return new Promise(resolve => setTimeout(resolve, time));
}

export async function orderbatchtest(HOST, USER, PASSWORD, DATABASE, BATCHSIZE, SENDSIZE, THREADS, TNAME) {
    const con = await mysql.createPool({
        host: HOST,
        user: USER,
        password: PASSWORD,
        database: DATABASE,
        connectionLimit: 100
    })
    if (!con) {
        console.error("No Pool Found!")
        process.exit(1);
    }
    /*
    let STARTVAL = (await lineitem.getnextorder(con))[0].nextorder;
    if (!STARTVAL) {
        console.error("No order found from lineitem!")
        process.exit(1);
    }
    */

    let threads = [];
    const totstart = Date.now();
    for (let i = 1; i <= THREADS; i++) {
        threads[i] = new Promise((resolve, reject) => {
            const start = Date.now();
            new Worker.Worker('./insorderworker.js', {
                workerData: {
                    host: HOST,
                    user: USER,
                    password: PASSWORD,
                    database: DATABASE,
                    batchsize: BATCHSIZE,
                    sendsize: SENDSIZE,
                    table: TNAME,
                    thread: i
                }
            }).on('message', (msg) => {
                //console.log(`Thread ${i} has a message`);
                //console.log(msg.thread);
                console.log(`Thread ${msg.thread} has received message in ${(Date.now() - start) / 1000.0} seconds.`)
                console.log(msg);
                resolve();
            }).on('error', (err) => {
                console.error(err)
                reject();
            }).on('exit', (code) => {
                if (code !== 0) {
                    console.log(`Thread ${i} has completed with code: ${code} in ${(Date.now() - start) / 1000.0} seconds.`)
                }
                resolve();
            })
        });
        await delay(20); //Avoid spiking connection creation
    }
    await Promise.allSettled(threads).then(() => {
        const tm = (Date.now() - totstart) / 1000.0;
        console.log(`Completed ${THREADS} Threads in ${tm} Seconds.`);
        console.log(`Insert Rows: ${THREADS * BATCHSIZE}`);
        console.log(`Rows/Second: ${(THREADS * BATCHSIZE) / tm}`);
        console.log(`Milliseconds per Row: ${(tm * 1000) / (THREADS * BATCHSIZE)}`);

    });
}
