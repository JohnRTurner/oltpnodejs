'use strict'
import wt from 'worker_threads'
import mysql from "mysql2/promise";
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
                rows = rows + await bullkInsert( details, wt.workerData.table,con);
                details = [];
            }
        }
        if(details.length > 0){
            rows = rows + await bullkInsert( details, wt.workerData.table, con);
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

async function bullkInsert(detailArray, table, conn) {
    // console.log('bulk insert called.');
    const [results] = await conn.query(
        'INSERT INTO ' + table + ' ' +
        '(\n' +
        '  id, shop_domain, app_id, billing_address, \n' +
        '  browser_ip, buyer_accepts_marketing, \n' +
        '  cancel_reason, cancelled_at, cart_token, \n' +
        '  checkout_token, client_details_user_agent, \n' +
        '  closed_at, created_at, currency, \n' +
        '  current_subtotal, current_total, \n' +
        '  customer_id, customer_locale, email, \n' +
        '  financial_status, location_id, name, \n' +
        '  note, note_attribute, number, order_number, \n' +
        '  order_status_url, original_total_duties_set, \n' +
        '  payment_gateway_names, presentment_currency, \n' +
        '  processed_at, processing_method, \n' +
        '  referring_site, shipping_address, \n' +
        '  source_identifier, source_name, \n' +
        '  source_url, subtotal_price, subtotal_price_set, \n' +
        '  taxes_included, total_discounts, \n' +
        '  total_discounts_set, total_line_items_price, \n' +
        '  total_line_items_price_set, total_price, \n' +
        '  total_price_set, total_price_usd, \n' +
        '  total_shipping_price_set, total_tax, \n' +
        '  total_tax_set, total_tip_received, \n' +
        '  total_weight, updated_at\n' +
        ') ' +
        'VALUES ? ' +
        'on duplicate key \n' +
        'update \n' +
        '  id = \n' +
        'VALUES \n' +
        '  (id), \n' +
        '  shop_domain = \n' +
        'VALUES \n' +
        '  (shop_domain), \n' +
        '  app_id = \n' +
        'VALUES \n' +
        '  (app_id), \n' +
        '  billing_address = \n' +
        'VALUES \n' +
        '  (billing_address), \n' +
        '  browser_ip = \n' +
        'VALUES \n' +
        '  (browser_ip), \n' +
        '  buyer_accepts_marketing = \n' +
        'VALUES \n' +
        '  (buyer_accepts_marketing), \n' +
        '  cancel_reason = \n' +
        'VALUES \n' +
        '  (cancel_reason), \n' +
        '  cancelled_at = \n' +
        'VALUES \n' +
        '  (cancelled_at), \n' +
        '  cart_token = \n' +
        'VALUES \n' +
        '  (cart_token), \n' +
        '  checkout_token = \n' +
        'VALUES \n' +
        '  (checkout_token), \n' +
        '  client_details_user_agent = \n' +
        'VALUES \n' +
        '  (client_details_user_agent), \n' +
        '  closed_at = \n' +
        'VALUES \n' +
        '  (closed_at), \n' +
        '  created_at = \n' +
        'VALUES \n' +
        '  (created_at), \n' +
        '  currency = \n' +
        'VALUES \n' +
        '  (currency), \n' +
        '  current_subtotal = \n' +
        'VALUES \n' +
        '  (current_subtotal), \n' +
        '  current_total = \n' +
        'VALUES \n' +
        '  (current_total), \n' +
        '  customer_id = \n' +
        'VALUES \n' +
        '  (customer_id), \n' +
        '  customer_locale = \n' +
        'VALUES \n' +
        '  (customer_locale), \n' +
        '  email = \n' +
        'VALUES \n' +
        '  (email), \n' +
        '  financial_status = \n' +
        'VALUES \n' +
        '  (financial_status), \n' +
        '  landing_site_base_url = \n' +
        'VALUES \n' +
        '  (landing_site_base_url), \n' +
        '  location_id = \n' +
        'VALUES \n' +
        '  (location_id), \n' +
        '  name = \n' +
        'VALUES \n' +
        '  (name), \n' +
        '  note = \n' +
        'VALUES \n' +
        '  (note), \n' +
        '  note_attribute = \n' +
        'VALUES \n' +
        '  (note_attribute), \n' +
        '  number = \n' +
        'VALUES \n' +
        '  (number), \n' +
        '  order_number = \n' +
        'VALUES \n' +
        '  (order_number), \n' +
        '  order_status_url = \n' +
        'VALUES \n' +
        '  (order_status_url), \n' +
        '  original_total_duties_set = \n' +
        'VALUES \n' +
        '  (original_total_duties_set), \n' +
        '  payment_gateway_names = \n' +
        'VALUES \n' +
        '  (payment_gateway_names), \n' +
        '  presentment_currency = \n' +
        'VALUES \n' +
        '  (presentment_currency), \n' +
        '  processed_at = \n' +
        'VALUES \n' +
        '  (processed_at), \n' +
        '  processing_method = \n' +
        'VALUES \n' +
        '  (processing_method), \n' +
        '  referring_site = \n' +
        'VALUES \n' +
        '  (referring_site), \n' +
        '  shipping_address = \n' +
        'VALUES \n' +
        '  (shipping_address), \n' +
        '  source_identifier = \n' +
        'VALUES \n' +
        '  (source_identifier), \n' +
        '  source_name = \n' +
        'VALUES \n' +
        '  (source_name), \n' +
        '  source_url = \n' +
        'VALUES \n' +
        '  (source_url), \n' +
        '  subtotal_price = \n' +
        'VALUES \n' +
        '  (subtotal_price), \n' +
        '  subtotal_price_set = \n' +
        'VALUES \n' +
        '  (subtotal_price_set), \n' +
        '  taxes_included = \n' +
        'VALUES \n' +
        '  (taxes_included), \n' +
        '  total_discounts = \n' +
        'VALUES \n' +
        '  (total_discounts), \n' +
        '  total_discounts_set = \n' +
        'VALUES \n' +
        '  (total_discounts_set), \n' +
        '  total_line_items_price = \n' +
        'VALUES \n' +
        '  (total_line_items_price), \n' +
        '  total_line_items_price_set = \n' +
        'VALUES \n' +
        '  (total_line_items_price_set), \n' +
        '  total_price = \n' +
        'VALUES \n' +
        '  (total_price), \n' +
        '  total_price_set = \n' +
        'VALUES \n' +
        '  (total_price_set), \n' +
        '  total_price_usd = \n' +
        'VALUES \n' +
        '  (total_price_usd), \n' +
        '  total_shipping_price_set = \n' +
        'VALUES \n' +
        '  (total_shipping_price_set), \n' +
        '  total_tax = \n' +
        'VALUES \n' +
        '  (total_tax), \n' +
        '  total_tax_set = \n' +
        'VALUES \n' +
        '  (total_tax_set), \n' +
        '  total_tip_received = \n' +
        'VALUES \n' +
        '  (total_tip_received), \n' +
        '  total_weight = \n' +
        'VALUES \n' +
        '  (total_weight), \n' +
        '  updated_at = \n' +
        'VALUES \n' +
        '  (updated_at)'
        , [detailArray]);
    return results.affectedRows;
};