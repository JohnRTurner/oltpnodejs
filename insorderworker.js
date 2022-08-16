'use strict'
import wt from 'worker_threads'
import mysql from "mysql2/promise";
import order from "./order.js";
import casual from "casual";

let con;
try {
    // console.log(`thread ${wt.workerData.thread} created.`);
    con = await mysql.createConnection({
        host:       wt.workerData.host,
        user:       wt.workerData.user,
        password:   wt.workerData.password,
        database:   wt.workerData.database,
        connectTimeout: 60000
    });
    let rows = 0;
    try {
        let details = [];
        let linenumber = 0;
        let sendsize = wt.workerData.sendsize;
        for(let i=0;i<wt.workerData.batchsize;i++) {
            const detail = [
                // id, shop_domain, app_id, billing_address,
                casual.integer(1, 1000000),'bones-coffee-company.myshopify.com', 294519,
                '{"first_name":"David","address1":"4036 S. 342nd St","phone":"2538861069","city":"Auburn","zip":"98001",' +
                '"province":"Washington","country":"United States","last_name":"Farrow","address2":null,"company":null,' +
                '"latitude":null,"longitude":null,"name":"David Farrow","country_code":"US","province_code":"WA"}',
                // browser_ip, buyer_accepts_marketing,
                null, false,
                // cancel_reason, cancelled_at, cart_token,
                null, null, null,
                // checkout_token, client_details_user_agent,
                null, null,
                // closed_at, created_at, currency,
                null, '2022-06-27T04:29:32Z', 'USD',
                // current_subtotal, current_total,
                null, null,
                // customer_id, customer_locale, email,
                6197000449, null,'dnfarrow@msn.com',
                // financial_status, location_id, name,
                'paid', null, '#1669736',
                // note, note_attribute, number, order_number,
                '', null, 1668736, 1668736,
                // order_status_url, original_total_duties_set,
                'https://www.bonescoffee.com/14755488/orders/dc7cdb626d9bee529e877a38d06ac748/authenticate?key=eb5486e141a8335fbaf81dfd2d45e3b0',
                null,
                // payment_gateway_names, presentment_currency,
                null, 'USD',
                // processed_at, processing_method,
                '2022-06-27T04:29:32Z', '',
                // referring_site, shipping_address,
                null, '{"first_name":"David","address1":"4036 S. 342nd St","phone":"2538861069","city":"Auburn",' +
                '"zip":"98001","province":"Washington","country":"United States","last_name":"Farrow","address2":null,' +
                '"company":null,"latitude":47.2958161,"longitude":-122.2819945,"name":"David Farrow","country_code":"US","province_code":"WA"}',
                // source_identifier, source_name,
                null, '294517',
                // source_url, subtotal_price, subtotal_price_set,
                null, '13.49', '{"shop_money":{"amount":"13.49","currency_code":"USD"},"presentment_money":{"amount":"13.49","currency_code":"USD"}}',
                // taxes_included, total_discounts,
                false, '0.00',
                // total_discounts_set, total_line_items_price,
                '{"shop_money":{"amount":"0.00","currency_code":"USD"},"presentment_money":{"amount":"0.00","currency_code":"USD"}}',
                '13.49',
                // total_line_items_price_set, total_price,
                '{"shop_money":{"amount":"13.49","currency_code":"USD"},"presentment_money":{"amount":"13.49","currency_code":"USD"}}',
                '20.48',
                // total_price_set, total_price_usd,
                '{"shop_money":{"amount":"20.48","currency_code":"USD"},"presentment_money":{"amount":"20.48","currency_code":"USD"}}',
                '20.48',
                // total_shipping_price_set, total_tax,
                '{"shop_money":{"amount":"6.99","currency_code":"USD"},"presentment_money":{"amount":"6.99","currency_code":"USD"}}',
                '0.00',
                // total_tax_set, total_tip_received,
                '{"shop_money":{"amount":"0.00","currency_code":"USD"},"presentment_money":{"amount":"0.00","currency_code":"USD"}}',
                '0.00',
                // total_weight, updated_at
                352, '2022-06-29T00:45:32Z'
                /*
                i + wt.workerData.startval,
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
                 */
            ];
            details.push(detail);
            if(details.length >= sendsize){
                rows = rows + await order.bullkInsert( details, wt.workerData.table,con);
                details = [];
            }
        }
        if(details.length > 0){
            rows = rows + await order.bullkInsert( details, wt.workerData.table, con);
        }
        wt.parentPort.postMessage({
            msg: `Inserted rows: ${rows}`,
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
