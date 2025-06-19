const { kafka } = require("./client");
const path = require('path')
const fs = require('fs/promises');

// Producer code that takes input from the user and sends it to the Kafka topic i.e emits the event 
async function init() {
    //parse the json data
    const rawData = await fs.readFile(path.join(__dirname, 'enriched_1000.ndjson'), 'utf-8');
    const lines = rawData.split('\n').filter(line => line.trim() !== '');
    const events = lines.map(line => JSON.parse(line));

    const producer = kafka.producer();

    console.log("Connecting Producer");
    await producer.connect();
    console.log("Producer Connected Successfully");
    
    const topicBuckets = {
        'account-events': [],
        'transaction-events': [],
        'audit-events': []
    };

    for (const event of events) {
        const { event_type } = event;

        if (event_type === 'AccountCreated') {
            topicBuckets['account-events'].push(event);
        } 
        else if(['MoneyDeposited', 'MoneyWithdrawn'].includes(event_type)) {
                topicBuckets['transaction-events'].push(event);
        }

        const auditEvent = {
            event_type: `${event_type}Audit`,
            account_id: event.account_id,
            event_id:event.event_id,
            performed_by: 'system',
            timestamp: event.timestamp ?? new Date().toISOString(),
            original_event: event // Store the actual event payload for traceability
        };

        topicBuckets['audit-events'].push(auditEvent);
    }

    const batchSize = 200;
    // Send in batches of 500 to each topic parallely
    async function sendBatch(topic,bucket,producer) {
    for(let i=0;i<bucket.length;i+=batchSize){
        const batch = bucket.slice(i,i+batchSize);

        const messages = batch.map(e => ({
            key: e.account_id,
            value: JSON.stringify(e)
        }));
        await producer.send({
            topic : topic,
            messages : messages
        })
            console.log(`Sent ${messages.length} messages to ${topic}`);
        }
    }

    // This runs all three function calls parallely and waits until all of it gets completed/fulfilled
    try {
        await sendBatch('account-events', topicBuckets['account-events'], producer);
        await Promise.all([
            sendBatch('transaction-events', topicBuckets['transaction-events'], producer),
            sendBatch('audit-events', topicBuckets['audit-events'], producer)
        ]);

        console.log('All events produced :)');
        console.log(`Summary:`);
        console.log(`Account Events: ${topicBuckets['account-events'].length}`);
        console.log(`Transactions: ${topicBuckets['transaction-events'].length}`);
        console.log(`Audit: ${topicBuckets['audit-events'].length}`);
    } catch (err) {
        console.error('Failed to send events:', err);
    } finally {
        await producer.disconnect();
    }
}

init()