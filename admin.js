const { kafka } = require("./client");

// This code is to show the creation of Kafka Topic by admin with decalration of number of partiotions
async function init() {
    const admin = kafka.admin();
    console.log("Admin connecting...");
    await admin.connect();
    console.log("Adming Connection Success...");

    console.log("Creating the Topics :");
    await admin.createTopics({
        topics: [
        {
            topic: "account-events",
            numPartitions: 3,
        },
        {
            topic: "transaction-events",
            numPartitions: 6,
        },
        {
            topic: "audit-events",
            numPartitions: 3,
        },
        ],
    });
    console.log("Topic Created Success !!");

    console.log("Disconnecting Admin..");
    await admin.disconnect();
}

init();