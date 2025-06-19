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
  const consumer = kafka.consumer({ groupId: "read-model-updaters11" });
  await consumer.connect();

  await consumer.subscribe({ topic: "account-events", fromBeginning: true });
  await consumer.subscribe({ topic: "transaction-events", fromBeginning: true });
  await consumer.subscribe({ topic: "audit-events", fromBeginning: true });

  
   await consumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      // now destructure the new fields too:
      const event = JSON.parse(message.value.toString());
      const {
        event_id,
        transaction_id = null,
        account_id,
        name,
        event_type,
        amount,
        timestamp
      } = event;

      if (topic === "account-events" && event_type === "AccountCreated") {
        // assumes users table now has an event_id column
        await pool.query(
          `INSERT INTO users
             (event_id, account_id, name, event_type, balance, created_at)
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (account_id) DO NOTHING`,
          [ event_id, account_id, name, event_type, amount, new Date(timestamp) ]
        );

        await heartbeat();
        logger.info(`UserCreated: ${name} (ID:${account_id}) evt:${event_id}`);
        return;
      }

      if (topic === "transaction-events") {
        // pull current balance
        const res = await pool.query(
          `SELECT balance FROM users WHERE account_id = $1`,
          [ account_id ]
        );
        if (res.rows.length === 0) {
          // no account yet – log for later
          await fs.appendFile(
            path.join(__dirname, 'missing_accounts.log'),
            `${new Date().toISOString()}  NO-USER  acc:${account_id} evt:${event_id}\n`
          );
          return;
        }

        const current    = parseFloat(res.rows[0].balance);
        const delta      = (event_type === "MoneyDeposited" ? +amount : -amount);
        const newBalance = current + delta;

        // update users balance
        await pool.query(
          `UPDATE users
              SET balance = $1
            WHERE account_id = $2`,
          [ newBalance, account_id ]
        );

        // insert into transactions, now storing event_id + transaction_id too
        await pool.query(
          `INSERT INTO transactions
             (event_id, transaction_id, account_id, transaction_type, amount, timestamp)
           VALUES ($1, $2, $3, $4, $5, $6)`,
          [ event_id, transaction_id, account_id, event_type, amount, new Date(timestamp) ]
        );

        await heartbeat();
        logger.info(
          `Txn ${event_type} ₹${amount} for ${account_id}. ` +
          `evt:${event_id} txn:${transaction_id} newBal:₹${newBalance}`
        );
        return;
      }

      if (topic === "audit-events") {
        const { event_type: auditType, account_id: acct, timestamp: ts, original_event } = event;
        await pool.query(
          `INSERT INTO audit_logs
             (event_id, event_type, account_id, created_at, raw_event)
           VALUES ($1, $2, $3, $4, $5)`,
          [ event_id, auditType, acct, new Date(ts), JSON.stringify(original_event) ]
        );
        await heartbeat();
        logger.info(`Audit log: ${auditType} acc:${acct} evt:${event_id}`);
        return;
      }

      // fallback heartbeat in case none of the above matched
      await heartbeat();
    }
  });
}

init();

