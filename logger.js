// logger.js
const { createLogger, format, transports } = require('winston');
const path = require('path');

const logger = createLogger({
  level: 'info',
  format: format.combine(
    format.timestamp(),
    format.printf(({ timestamp, level, message }) => {
      return `${timestamp} [${level.toUpperCase()}] ${message}`;
    })
  ),
  transports: [
    // write all logs to consumer.log
    new transports.File({ filename: path.join(__dirname, 'consumer.log') }),
    // also print them to the console
    new transports.Console()
  ]
});

module.exports = logger;
