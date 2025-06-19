const { Kafka } = require("kafkajs");
const path = require("path");
const fs = require("fs");      
require('dotenv').config();              
const caCert = fs.readFileSync(
    path.join(__dirname, "ca.pem")
);
const password = process.env.AIVEN_PASSWORD;
const uname = process.env.AIVEN_USERNAME;

console.log(password)
console.log(uname)

exports.kafka = new Kafka({
    clientId: "my-app",
    brokers: ["kafka-2dbff1a3-kafka.b.aivencloud.com:12520"],
    ssl: {
        ca: [caCert],                          
    },
    sasl: {
        username: uname,
        password: password,
        mechanism: "plain",
    },
});


