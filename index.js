const express = require('express')
const app = express()
const bodyParser = require('body-parser')

const r = require('rethinkdb-reconnect')

let database = null
function connectToDatabase() {
  if(!database) {
    database = r.autoConnect({
      host: process.env.DB_HOST,
      port: process.env.DB_PORT,
      db: process.env.DB_NAME,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      timeout: process.env.DB_TIMEOUT,
    })
  }
  return database
}

(async () => {
  const db = connectToDatabase()
  await Promise.all([
    db.run(r.tableCreate("clientLogs")).catch(e=>{}),
    db.run(r.tableCreate("clientLogsMessages")).catch(e=>{})
  ])
  await Promise.all([
    db.run(r.table("clientLogs").indexCreate("sessionId")).catch(e=>{}),
    db.run(r.table("clientLogs").indexCreate("windowsId")).catch(e=>{}),
    db.run(r.table("clientLogs").indexCreate("userId")).catch(e=>{}),
    db.run(r.table("clientLogs").indexCreate("date")).catch(e=>{}),
    db.run(r.table("clientLogs").indexCreate("tags")).catch(e=>{}),
    db.run(r.table("clientLogsMessages").indexCreate("logId")).catch(e=>{}),
    db.run(r.table("clientLogsMessages").indexCreate("logIdTs", [r.row("logId"), r.row("timestamp")])).catch(e=>{})
  ])
})()

function getUserId(sessionId) {
  return connectToDatabase().then(
    conn => db.run(r.table("session").get(sessionId))
  ).then(
    session => session && session.userId
  )
}

app.get('/', function (req, res) {
  res.send('Log server')
})

app.use(bodyParser.json());


app.post('/saveLogs', function(req, res) {
  let ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress
  res.contentType('json')
  const db = connectToDatabase()
  let msg = req.body
  //console.log("HANDLE MSG", Object.keys(msg))
  let sessionId = msg.sessionId
  let windowId = msg.windowId
  return getUserId(sessionId).then(userId => {
    let logId = sessionId + "_" + windowId
    let logs = msg.logs
    return Promise.all([
      db.run(
        r.table('clientLogsMessages').insert({
          logId,
          timestamp: logs[0].timestamp,
          logs: logs.map(log => JSON.stringify(log))
        })
      ),
      db.run(
        r.table('clientLogs').insert({
          sessionId, windowId,
          userId: userId || null,
          ip, logId,
          lastTimestamp: logs[logs.length - 1].timestamp,
          date: new Date(),
          id: logId,
          tags: msg.tags
        }, {conflict: "update"})
      )
    ]).then(saved => {
      res.send(JSON.stringify({ result: "logsSaved" }))
    })
  }).catch(error => {
    res.send(JSON.stringify({ error: ""+error }))
  })
})


let port = process.env.LOG_SERVER_PORT || 8709

require("../config/metricsWriter.js")('log-server', () => ({

}))

app.listen(port, function () {
  console.log(`Log server listening on port ${port}!`)
})