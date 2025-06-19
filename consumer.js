const { kafka } = require("./client");
const { Pool } = require("pg");
const logger = require("./logger")
const fs = require('fs/promises');
const path = require('path');

// This is the consumer code that will be executed in the consumer group

const pool = new Pool({
  user: "postgres",
  host: "localhost",
  database: "banking_system",
  password: "1234",
  port: 5432,
});

async function init() {
  const consumer = kafka.consumer({ groupId: "read-model-updaters" });
  await consumer.connect();

  await consumer.subscribe({ topic: "account-events", fromBeginning: true });
  await consumer.subscribe({ topic: "transaction-events", fromBeginning: true });
  await consumer.subscribe({ topic: "audit-events", fromBeginning: true });

  
await consumer.run({
  eachMessage: async ({ topic, partition, message,heartbeat }) => {
    const event = JSON.parse(message.value.toString());
    const { account_id, name, event_type, amount, timestamp } = event;

    if (topic === "account-events" && event_type === "AccountCreated") {
      await pool.query(
        `INSERT INTO users (account_id, name, event_type, balance, created_at)
          VALUES ($1, $2, $3, $4, $5)
          ON CONFLICT (account_id) DO NOTHING`,
        [account_id, name, event_type, amount, new Date(timestamp)]
      );
      await heartbeat();
      console.log(`Created user: ${name} (ID: ${account_id}) with ₹${amount}`);
      logger.info(`Created user: ${name} (ID: ${account_id}) with ₹${amount}`);
    }
    else if (topic === "transaction-events") {
      const select = await pool.query(
        `SELECT balance FROM users WHERE account_id = $1`,
        [account_id]
      );
      if (!select.rows.length) {
        console.warn(`No user found for ID ${account_id}`);

        // Log it to a file for later inspection
        await fs.appendFile(
          path.join(__dirname, 'missing_accounts.log'),
          `${account_id} - ${event_type}\n`
        );

        return;
      }

      // force numeric math
      const current    = parseFloat(select.rows[0].balance);
      const amountNum  = Number(amount);
      const delta      = (event_type === "MoneyDeposited" ? amountNum : -amountNum);
      const newBalance = current + delta;

      await pool.query(
        `UPDATE users SET balance = $1 WHERE account_id = $2`,
        [newBalance, account_id]
      );
      await pool.query(
        `INSERT INTO transactions
          (account_id, transaction_type, amount, timestamp)
        VALUES ($1,$2,$3,$4)`,
        [account_id, event_type, amountNum, event.timestamp]
      );

      console.log(
        `${event_type} ₹${amountNum} for ${account_id}. New balance: ₹${newBalance}`
      );

      logger.info(
        `${event_type} ₹${amountNum} for ${account_id}. New balance: ₹${newBalance}`
      );

      // finally heartbeat so Kafka knows you're still alive
      await heartbeat();
    }
    else if (topic === "audit-events") {
      const { event_type: auditType, account_id: acct, timestamp: ts, original_event } = event;
      await pool.query(
        `INSERT INTO audit_logs (event_type, account_id, created_at, raw_event)
        VALUES ($1, $2, $3, $4)`,
        [auditType, acct, new Date(ts), JSON.stringify(original_event)]
      );
      await heartbeat();
      console.log(`Audit log added: ${auditType} for ${acct}`);
      logger.info(`Audit log added: ${auditType} for ${acct}`);
    }
    console.log(` [${topic}] PART:${partition} -> ${message.value.toString()}`);
    await heartbeat();
    }
  });
}

init();

