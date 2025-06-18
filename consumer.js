const { kafka } = require("./client");
const { Pool } = require("pg");
// This is the consumer code that will be executed in the consumer group

const pool = new Pool({
  user: "postgres",
  host: "localhost",
  database: "banking_system",
  password: "1234",
  port: 5432,
});

async function init() {
  const consumer = kafka.consumer({ groupId: "read-model-updater" });
  await consumer.connect();

  await consumer.subscribe({ topic: "account-events", fromBeginning: true });
  await consumer.subscribe({ topic: "transaction-events", fromBeginning: true });
  await consumer.subscribe({ topic: "audit-events", fromBeginning: true });

  
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    const event = JSON.parse(message.value.toString());
    const { account_id, name, event_type, amount, timestamp } = event;

    if (topic === "account-events" && event_type === "AccountCreated") {
      await pool.query(
        `INSERT INTO users (account_id, name, event_type, balance, created_at)
          VALUES ($1, $2, $3, $4, $5)
          ON CONFLICT (account_id) DO NOTHING`,
        [account_id, name, event_type, amount, new Date(timestamp)]
      );
      console.log(`Created user: ${name} (ID: ${account_id}) with ₹${amount}`);
    }
    else if (topic === "transaction-events") {
      const res = await pool.query(
        `SELECT balance FROM users WHERE account_id = $1`,
        [account_id]
      );
      if (res.rows.length === 0) {
        console.warn(`No user found for ID ${account_id}`);
        return;
      }

      let newBalance = res.rows[0].balance +
        (event_type === "MoneyDeposited" ? amount : -amount);

      await pool.query(
        `UPDATE users SET balance = $1 WHERE account_id = $2`,
        [newBalance, account_id]
      );
      await pool.query(
        `INSERT INTO transactions (account_id, transaction_type, amount, timestamp)
        VALUES ($1, $2, $3, $4)`,
        [account_id, event_type, amount, new Date(timestamp)]
      );
      console.log(`${event_type} of ₹${amount} for ${account_id}. New balance: ₹${newBalance}`);
    }
    else if (topic === "audit-events") {
      const { event_type: auditType, account_id: acct, timestamp: ts, original_event } = event;
      await pool.query(
        `INSERT INTO audit_logs (event_type, account_id, created_at, raw_event)
        VALUES ($1, $2, $3, $4)`,
        [auditType, acct, new Date(ts), JSON.stringify(original_event)]
      );
      console.log(`Audit log added: ${auditType} for ${acct}`);
    }

    console.log(` [${topic}] PART:${partition} -> ${message.value.toString()}`);
    }
  });
}

init();

