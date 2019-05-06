const express = require('express')
const app = express()
const bodyParser = require('body-parser')

const r = require('rethinkdb')

let databasePromise = null
function connectToDatabase() {
  if(!databasePromise) {
    databasePromise = r.connect({
      host: process.env.DB_HOST,
      port: process.env.DB_PORT,
      db: process.env.DB_NAME,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      timeout: process.env.DB_TIMEOUT,
    })
  }
  return databasePromise
}

function getUserId(sessionId) {
  return connectToDatabase().then(
    conn => r.table("session").get(sessionId).run(conn)
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
  res.contentType('json');
  connectToDatabase().then(db => {
    let msg = req.body
    //console.log("HANDLE MSG", Object.keys(msg))
    let sessionId = msg.sessionId
    let windowId = msg.windowId
    return getUserId(sessionId).then(userId => {
      let logId = sessionId + "_" + windowId
      let logs = msg.logs
      return Promise.all([
        r.table('clientLogsMessages').insert({
          logId,
          timestamp: logs[0].timestamp,
          logs: logs.map(log => JSON.stringify(log))
        }).run(db),
        r.table('clientLogs').insert({
          sessionId, windowId,
          userId: userId || null,
          ip, logId,
          lastTimestamp: logs[logs.length - 1].timestamp,
          date: new Date(),
          id: logId,
          tags: msg.tags
        }, {conflict: "update"}).run(db)
      ]).then(saved => {
        res.send(JSON.stringify({ result: "logsSaved" }))
      })
    })
  }).catch(error => {
    res.send(JSON.stringify({ error: ""+error }))
  })
})


let port = process.env.LOG_SERVER_PORT || 8709

app.listen(port, function () {
  console.log(`Log server listening on port ${port}!`)
})