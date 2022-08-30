'use strict'

import {lineitemoltptest} from "../lineitem/lineitemoltptest.js";
import {lineitembatchtest} from "../lineitem/lineitembatchtest.js";
import {orderbatchtest} from "../order_3fish/orderbatchtest.js";

import app from "../app.js";
//var debug = require('debug')('expresswebtest:server');
import debug from "debug";
import http from "http";
import status from "../data/status.js";



const HOST = process.env.DBHOST || 'svc-3f97fbaa-99ad-4b51-bdab-b44e14848132-dml.aws-virginia-2.svc.singlestore.com';
// const HOST = process.env.DBHOST || 'svc-9739659c-fe84-4249-ae51-694c8d07805b-dml.aws-virginia-4.svc.singlestore.com';
const USER = process.env.DBUSER || 'admin';
const PASSWORD = process.env.DBPASS || '!!!!!';
const DATABASE = process.env.DBDATABASE || 'tpchtest';
//const DATABASE = process.env.DBDATABASE || 'shopify';
const TOTALSIZE = process.env.BATCHSIZE || 80000;
const SENDSIZE = process.env.SENDSIZE || 1000;
const THREADS = process.env.THREADS || 8;

// Hidden parameters for now...
const MAXLINES = process.env.MAXLINES || 10;
const MAXPARTKEY = process.env.MAXPARTKEY || 20000000;
const MAXSUPPKEY = process.env.MAXSUPPKEY || 1000000;
const MINPRICE = process.env.MINPRICE || 900.00;
const MAXPRICE = process.env.MAXPRICE || 104947.00;

const LINEOLTPTEST = process.env.LINEOLTPTEST || 0;
const LINEBATCHTEST = process.env.LINEBATCHTEST || 1;

const JSONTEST = process.env.JSON || 0;

const ORDERTEST = process.env.ORDERTEST || 0;
const TNAME = process.env.TNAME || 'order_v3';

const WWWTEST = process.env.WWWTEST || 0;

let server;


/**
 * Normalize a port into a number, string, or false.
 */

function normalizePort(val) {
    const port = parseInt(val, 10);

    if (isNaN(port)) {
        // named pipe
        return val;
    }

    if (port >= 0) {
        // port number
        return port;
    }

    return false;
}

/**
 * Event listener for HTTP server "error" event.
 */

function onError(error) {
    if (error.syscall !== 'listen') {
        throw error;
    }

    const bind = typeof port === 'string'
        ? 'Pipe ' + port
        : 'Port ' + port;

    // handle specific listen errors with friendly messages
    switch (error.code) {
        case 'EACCES':
            console.error(bind + ' requires elevated privileges');
            process.exit(1);
            break;
        case 'EADDRINUSE':
            console.error(bind + ' is already in use');
            process.exit(1);
            break;
        default:
            throw error;
    }
}

/**
 * Event listener for HTTP server "listening" event.
 */

function onListening() {
    const addr = server.address();
    const bind = typeof addr === 'string'
        ? 'pipe ' + addr
        : 'port ' + addr.port;
    debug('Listening on ' + bind);
}

if(LINEOLTPTEST){
    await lineitemoltptest(HOST, USER, PASSWORD, DATABASE, JSONTEST);
}

if(LINEBATCHTEST) {
    let batchsize = TOTALSIZE / THREADS
    lineitembatchtest(HOST, USER, PASSWORD, DATABASE, batchsize, SENDSIZE, THREADS,
        MAXLINES, MAXPARTKEY, MAXSUPPKEY, MINPRICE, MAXPRICE, JSONTEST);
    await status.delay(1000);
    let readstats = 0;
    while(status.getStatus() !== 'Idle'){
        let updatelst = status.getUpdates(readstats);
        readstats += updatelst.length;
        console.log({'Status': status.getStatus(), 'Updates': updatelst});
        await status.delay(3000);
    }
    console.log({'Status': status.getStatus(), 'Updates': status.getUpdates(readstats)});
}

if(ORDERTEST) {
    let batchsize = TOTALSIZE / THREADS
    console.log(`Starting test on ${TNAME}`);
    await orderbatchtest(HOST, USER, PASSWORD, DATABASE, batchsize, SENDSIZE, THREADS, TNAME);
}

if(WWWTEST){
    const port = normalizePort(process.env.PORT || '8888');
    app.set('port', port);
    server = http.createServer(app);
    server.listen(port);
    server.on('error', onError);
    server.on('listening', onListening);
} else {
    console.log("Exiting");
    process.exit(0);
}


