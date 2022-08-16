'use strict'

import {lineitemjsonoltptest} from "./lineitemjsonoltptest.js";
import {lineitemjsonbatchtest} from "./lineitemjsonbatchtest.js";
import {lineitemoltptest} from "./lineitemoltptest.js";
import {lineitembatchtest} from "./lineitembatchtest.js"

const HOST = process.env.DBHOST || 'svc-3f97fbaa-99ad-4b51-bdab-b44e14848132-dml.aws-virginia-2.svc.singlestore.com';
const USER = process.env.DBUSER || 'admin';
const PASSWORD = process.env.DBPASS || '';
const DATABASE = process.env.DBDATABASE || 'tpchtest';
const BATCHSIZE = process.env.BATCHSIZE || 100000;
const SENDSIZE = process.env.SENDSIZE || 10000;
const THREADS = process.env.THREADS || 8;

// Hidden parameters for now...
const MAXLINES = process.env.MAXLINES || 10;
const MAXPARTKEY = process.env.MAXPARTKEY || 20000000;
const MAXSUPPKEY = process.env.MAXSUPPKEY || 1000000;
const MINPRICE = process.env.MINPRICE || 900.00;
const MAXPRICE = process.env.MAXPRICE || 104947.00;

const OLTPTEST = process.env.MAXPRICE || 0;
const BATCHTEST = process.env.MAXPRICE || 1;

const JSONTEST = process.env.JSON || 0;

if(OLTPTEST && JSONTEST){
    await lineitemjsonoltptest(HOST, USER, PASSWORD, DATABASE);
}

if(BATCHTEST && JSONTEST) {
    await lineitemjsonbatchtest(HOST, USER, PASSWORD, DATABASE, BATCHSIZE, SENDSIZE, THREADS,
        MAXLINES, MAXPARTKEY, MAXSUPPKEY, MINPRICE, MAXPRICE);
}

if(OLTPTEST && (!JSONTEST)){
    await lineitemoltptest(HOST, USER, PASSWORD, DATABASE);
}

if(BATCHTEST && (!JSONTEST)) {
    await lineitembatchtest(HOST, USER, PASSWORD, DATABASE, BATCHSIZE, SENDSIZE, THREADS,
        MAXLINES, MAXPARTKEY, MAXSUPPKEY, MINPRICE, MAXPRICE);
}


console.log("Exiting");
process.exit(0);