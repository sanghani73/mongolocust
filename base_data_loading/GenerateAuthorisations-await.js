/**
 * This script will generate data to be loaded into an "authorisations" collection.
 * The authorisation document represented here will be for merchants with an id of 30001 or 30002 (randomly assigned)
 * 
 * To run, you will need to supply the URI of the database and the number of documents to load
 *  e.g. node GenerateAuthorisations.js "mongodb+srv://admin:****@ananddev.hluaz.mongodb.net/fiservJS" 100000
 * 
 */

//  var chance = require('chance').Chance();
var mgenerate = require('mgeneratejs');
const { EJSON } = require('bson');
var MongoClient = require('mongodb').MongoClient;
const BATCH_SIZE=1000

async function generateAuth() {
    return mgenerate(
       {
           "auth_id": "$objectid",
           "merch_id": {"$number": {"min": 30001, "max": 30002}},
           "alliance_code" : "CODE123",
           "bank_name": "bank_name",
           "bank_country": "$country",
           "posting_date" : {"$date": {"min": "2020-01-01", "max": "2020-12-31"}},
           "merchant_details": {
               "external_merch_id": {"$number": {"min": 100000, "max": 1000000}},
               "custom_merch_id": {"$number": {"min": 100000, "max": 1000000}},
               "merch_type": {"$string": {"length": 6, "symbols": false, "alpha": true, "casing": "upper"}},
               "name": "$company",
               "trade_name": "$company",
               "mcc_iso": {"$number": {"min": 100000, "max": 1000000}},
               "partner_code": {"$join": {"array": [{"$string": {"length": 3, "symbols": false, "alpha": true, "casing": "upper"}}, {"$number": {"min":10000, "max":20000}}]}}
           },
           "agent_code": {"$number": {"min": 100000, "max": 1000000}},
           "platform_code": {"$number": {"min": 100000, "max": 1000000}},
           "instrument_details": {
               "instrument_num": {"$number": {"min": 100000, "max": 1000000}},
               "instrument_num_masked": {"$number": {"min": 1000000000000000, "max": 9000000000000000}},
               "instrument_type_code" : {"$string": {"length": 3, "symbols": false, "alpha": true, "casing": "upper"}},
               "instrument_type_subcode" : {"$string": {"length": 3, "symbols": false, "alpha": true, "casing": "upper"}}
           },
           "auth_details": {
               "auth_code" : {"$string": {"length": 9, "symbols": false, "alpha": true, "casing": "upper"}},
               "auth_date" : {"$date": {"min": "2020-01-01", "max": "2020-12-31"}},
               "auth_exp" : {"$date": {"min": "2020-01-01", "max": "2020-12-31"}},
               "auth_flag": "AA",
               "auth_id": "$objectid",
               "auth_ref_id": {"$number": {"min": 100000, "max": 1000000}},
               "auth_seq": {"$number": {"min": 1, "max": 10}},
               "auth_time": {"$join": {"array": [{"$number": {"min":0, "max":24}},":",{"$number": {"min":0, "max":60}},":",{"$number": {"min":0, "max":60}}]}},
               "auth_method": {"$choose": {"from": ["ABC","123","XYZ"]}},
               "auth_method_code": {"$number": {"min": 1021, "max": 2131}},
               "auth_platform_code": {"$number": {"min": 1021, "max": 2131}}
           },
           "acquirer_details" : {
               "merch_id": {"$number": {"min": 901000, "max": 901030}},
               "resp_code": {"$number": {"min": 9001, "max": 9999}},
               "resp_code_desc": "$string",
               "terminal_id": {"$number": {"min": 301000, "max": 301030}},
               "acquired_id": {"$number": {"min": 401000, "max": 401030}},
               "bin": {"$string": {"length": 4, "symbols": false, "alpha": true, "casing": "upper"}},
               "token": "$objectid"
           },
           "account_details": {
               "account_type": {"$choose": {"from": ["Credit","Debit","chequing"],"weights": [10, 10, 1]}},
               "account_type_code": {"$choose": {"from": ["CR1213","DB1212","CQ1213"],"weights": [10, 10, 1]}},
               "account_update_service_flag": {"$string": {"length": 2, "symbols": false, "alpha": true, "casing": "upper"}}
           },
           "card_details" : {
               "card_classification" : {"$string": {"length": 6, "symbols": false, "alpha": true, "casing": "mixed"}},
               "card_curr_code": {"$choose": {"from": ["USD","GBP","EUR"],"weights": [1,10,10] }},
               "card_holder_name": "$name",
               "card_service_type_code": {"$string": {"length": 4, "symbols": false, "alpha": true, "casing": "upper"}},
               "card_usage_code": {"$string": {"length": 4, "symbols": false, "alpha": true, "casing": "upper"}},
               "card_verification": {"$string": {"length": 6, "symbols": false, "alpha": true, "casing": "mixed"}},
               "card_verification_code": {"$string": {"length": 4, "symbols": false, "alpha": true, "casing": "upper"}},
               "cardholder_email": "$email",
               "card_present_indicator": {"$choose": {"from": ["N","Y"],"weights": [4,10] }}
           },
           "transaction_details": {
               "source_code": {"$string": {"length": 1, "symbols": false, "alpha": true, "casing": "upper"}},
               "location": {"$string": {"length": 3, "symbols": false, "alpha": true, "casing": "upper"}},
               "type": {"$string": {"length": 10, "symbols": false, "alpha": true, "casing": "upper"}},
               "transmission_date" : {"$date": {"min": "2020-01-01", "max": "2020-12-31"}},
               "transmission_time": {"$join": {"array": [{"$number": {"min":0, "max":24}},":",{"$number": {"min":0, "max":60}},":",{"$number": {"min":0, "max":60}}]}},
               "merchant_tran_ref": {"$number": {"min": 100000, "max": 1000000}},
               "purchase_amount": {"$floating": { "min": 0, "max": 1000, "fixed": 2 }},
               "chashback_amount": {"$floating": { "min": 0, "max": 10, "fixed": 2 }},
               "tip": "$bool",
               "tip_amount": {"$floating": { "min": 0, "max": 10, "fixed": 2 }}
           },
           "store_id": {"$number": {"min": 901010, "max": 902020}},
           "order_id": {"$number": {"min": 901010, "max": 902020}},
           "terminal_details": {
               "terminal_id": {"$number": {"min": 1201010, "max": 3402020}},
               "ip_address": "$ip",
               "terminal_curr_code": {"$choose": {"from": ["USD","GBP","EUR"],"weights": [1,10,10] }},
               "terminal_batch_num": {"$number": {"min": 1201010, "max": 3402020}},
               "pos_entry": {"$string": {"length": 2, "symbols": false, "alpha": true, "casing": "upper"}},
               "pos_capability": {"$string": {"length": 3, "symbols": false, "alpha": true, "casing": "lower"}}
           }
       })
}

async function insertData(URI, numberOfDocs) {
   const client = await MongoClient.connect(URI, { useNewUrlParser: true })
        .catch(err => { console.log(err); });

    if (!client) {
        return;
    }
    try {
        const db = client.db('fiservTest');
        var batch = db.collection('authorisations').initializeUnorderedBulkOp();
        for (var i = 0; i < numberOfDocs; ++i) {
            var auth = generateAuth()
            batch.insert(auth);
        }
        // Execute the operations
        let res = await batch.execute()
        // console.log(res);
    } catch (err) {
        console.log(err);
    } finally {
        client.close();
    }
}

async function load() {
    var myArgs = process.argv.slice(2);
    if (myArgs.length != 2) {
        throw("Need to to supply a URI as the first parameter and number of documents to load as the second")
    }
    var URI = myArgs[0]
    if  (typeof URI === "undefined") {
        throw("Need to to supply a URI as the first parameter")
    }
    var numberOfDocs = myArgs[1]
    if  (typeof numberOfDocs === "undefined") {
        numberOfDocs = 1000
    }
    var counter = Math.ceil(numberOfDocs/BATCH_SIZE)
    console.log("loading "+numberOfDocs+" in "+counter+" batches of "+BATCH_SIZE+"\n")
    for (var j=0; j<counter; j++){
        var before = new Date()
        let res = await insertData(URI, numberOfDocs)
        var after = new Date()
        execution_mills = after - before
        console.log("Total time for batch "+ j+ " = "+execution_mills+" (ms)")
    }
}

load()
