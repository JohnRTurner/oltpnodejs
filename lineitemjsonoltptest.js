// main is run at the end
import lineitemjson from "./lineitemjson.js";
import mysql from "mysql2/promise";
import casual from "casual";

export async function lineitemjsonoltptest(HOST, USER, PASSWORD, DATABASE) {
    await lineitemjson.precheck(HOST, USER, PASSWORD, DATABASE);
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

    let detail = {
        "l_orderkey" : 1,
        "l_partkey":casual.integer(1, 1000),
        "l_suppkey":casual.integer(1, 1000),
        "l_linenumber": 1,
        "l_quantity":casual.integer(1,100),
        "l_extendedprice":casual.double(100.0, 1000.0),
        "l_discount":casual.double(0.0, 10.0),
        "l_tax":casual.double(0.0,10.0),
        "l_returnflag":casual.letter,
        "l_linestatus":casual.letter,
        "l_shipdate":casual.date("YYYY-MM-DD"),
        "l_commitdate":casual.date("YYYY-MM-DD"),
        "l_receiptdate":casual.date("YYYY-MM-DD"),
        "l_shipinstruct":casual.string,
        "l_shipmode":casual.short_description,
        "l_comment":casual.description
    };

    try {
        console.log("You have successfully connected to SingleStore.");

        let rows = await lineitemjson.deleteorder(1, con);
        console.log(`Deleted rows: ${rows}`);


        try {
            let rows = await lineitemjson.insert(detail, con);
            console.log(`Inserted rows: ${rows}`);
        } catch (err) {
            if (err.code === 'ER_DUP_ENTRY') {
                console.log('Inserted Row Already Exists.')
            } else {
                console.log(err)
            }
        }

        let lineitems = await lineitemjson.getbyorderline(1, 1, con);
        if (lineitems === undefined) {
            console.log("Could not find Lineitems");
        } else {
            console.log(`Found Lineitems: ${JSON.stringify(lineitems)}`);
        }
        lineitems = await lineitemjson.getbyorder(1, con);
        if (lineitems === undefined) {
            console.log("Could not find Lineitems");
        } else {
            console.log(`Found Lineitems: ${JSON.stringify(lineitems)}`);
        }

        detail.otherfield = "updated field";
        rows = await lineitemjson.update(detail, con);
        console.log(`Updated rows: ${rows}`);

        lineitems = await lineitemjson.getbyorderline(1, 1, con);
        if (lineitems === undefined) {
            console.log("Could not find Lineitems");
        } else {
            console.log(`Found Lineitems: ${JSON.stringify(lineitems)}`);
        }

        rows = await lineitemjson.delete(1, 1, con);
        console.log(`Deleted rows: ${rows}`);

        lineitems = await lineitemjson.getbyorder(1, con);
        if (lineitems === undefined) {
            console.log("Could not find Lineitems");
        } else {
            console.log(`Found Lineitems: ${JSON.stringify(lineitems)}`);
        }

        lineitems = await lineitemjson.getbyordernojson(27001730, con);
        if (lineitems === undefined) {
            console.log("Could not find Lineitems");
        } else {
            lineitems.forEach((x) => {
                console.log(`Found Lineitem: ${JSON.stringify(x)}`);
            })
        }


        lineitems = await lineitemjson.getbyorder(27001730, con);
        if (lineitems === undefined) {
            console.log("Could not find Lineitems");
        } else {
            console.log(`Found Lineitems: ${JSON.stringify(lineitems)}`);
        }

        lineitems = await lineitemjson.getbyorderline(27001730, 1, con);
        if (lineitems === undefined) {
            console.log("Could not find Lineitems");
        } else {
            console.log(`Found Lineitems: ${JSON.stringify(lineitems)}`);
        }

        lineitems = await lineitemjson.getbyordernojson(27001730, con);
        if (lineitems === undefined) {
            console.log("Could not find Lineitems");
        } else {
            lineitems.forEach((x) => {
                console.log(`Found Lineitem: ${JSON.stringify(x)}`);
            })
        }

        /*
        lineitems = await lineitem.getbypart(16024499, con);
        if (lineitems === undefined){
            console.log("Could not find Lineitems");
        } else {
            console.log(`Found Lineitems: ${JSON.stringify(lineitems)}`);
        }

        lineitems = await lineitem.getbysupplier(999737, con);
        if (lineitems === undefined){
            console.log("Could not find Lineitems");
        } else {
            console.log(`Found Lineitems: ${JSON.stringify(lineitems)}`);
        }
    */
    } catch (err) {
        console.error('ERROR', err);
        process.exit(1);
    }

    if (con) {
        await con.destroy;
        console.log("Release Connection.")
    }

}