"use strict"

const mysql = require('mysql')
const util = require('util')

function Database(host, port, user, password, database, cb) {
  this.schema = {
    tables: {}
  }

  this.db = mysql.createConnection({ host: host, port: port, user: user, password: password, database: database })
  this.db.connect((error) => {
    if(error)
      return

    this.getSchemaInformation(cb)
  })

  this.getSchemaInformation = function(cb) {
    let count = 0
    this.db.query('SHOW TABLES', (e, r) => {
      count = r.length
      r.map(tr => tr[Object.keys(tr).pop()]).forEach(table => {
        this.schema[table] = {
          fields: [],
          pk: []
        }

        this.db.query('SHOW FIELDS FROM ??', [table], (e, r) => {
          r.forEach(f => {
            if(f.Key.toUpperCase() == 'PRI')
              this.schema[table].pk.push(f.Field)
            this.schema[table].fields.push(f.Field)
          })

          this[table] = this.generateRecordClass(table)

          count--
          if(count == 0 && cb)
            cb()
        })
      })
    })
  }

  this.query = function() {
    this.db.query.apply(this.db, arguments)
  }

  this.generateRecordClass = function(table) {
    let db = this
    return function(_pkValues) {
      let args = new Array(arguments.length)
      for(let n = 0; n < arguments.length; n++)
        args[n] = arguments[n]

      let self = this
      let pk = db.schema[table].pk
      let fields = db.schema[table].fields
      let lastSelectResult = undefined

      // create a writable property for each field
      fields.forEach((field) => {
        Object.defineProperty(this, field, { value: undefined, writable: true })
      })

      // setup initial primary key values
      if(util.isObject(args[0])) {
        Object.keys(args[0]).forEach((key, index) => {
          self[key] = args[0][key]
        })
      } else {
        let pkValues = util.isArray(args[0]) ? args[0] : args
        pkValues.forEach((value, index) => {
          if(index < pk.length)
            self[pk[index]] = value
        })
      }

      /**
       * Insert or update record
       * @param  {Function} cb Callback function
       */
      let saveRecord = function(cb) {
        let keysSet = pk.filter(keyField => self[keyField]).length
        let update = keysSet == pk.length

        if(update) {
          // generate UPDATE sql query
          let sqlSetValues = fields.map(fieldName => self[fieldName])
          let sqlSetList = fields.map(fieldName => util.format('%s = ?', fieldName)).join(', ')
          let sqlWhereValues = pk.map(fieldName => self[fieldName])
          let sqlWhereList = pk.map(fieldName => util.format('%s = ?', fieldName)).join(', ')
          let sql = util.format('UPDATE %s SET %s WHERE %s', table, sqlSetList, sqlWhereList)

          // execute
          db.query(sql, sqlSetValues.concat(sqlWhereValues), (error, result) => {
            if(error)
              return cb ? cb(error) : undefined
            if(cb) cb(result)
          })
        } else {
          // generate INSERT sql query
          let sqlFieldList = fields.filter(fieldName => self[fieldName])
          let sqlQuestionMarks = sqlFieldList.map(_ => '?').join(', ')
          let sqlValues = sqlFieldList.map(fieldName => self[fieldName])
          let sql = util.format('INSERT INTO %s (%s) VALUES (%s)', table, sqlFieldList, sqlQuestionMarks)

          // execute
          db.query(sql, sqlValues, (error, result) => {
            if(error)
              return cb ? cb(error) : undefined
            lastSelectResult = result
            self.next()

            if(cb) cb(result)
          })
        }
      }

      /**
       * Select records by using currently set values as filter
       * @param  {Function} cb Callback function
       */
      let selectRecords = function(cb) {
        let sqlWhereValues = fields.filter(fieldName => self[fieldName]).map(fieldName => self[fieldName])
        let sqlWhereList = fields.filter(fieldName => self[fieldName]).map(fieldName => util.format('%s = ?', fieldName)).join(' AND ')
        let sql = util.format('SELECT * FROM %s', table)
        if(sqlWhereValues.length > 0)
          sql += util.format(' WHERE %s', sqlWhereList)

        db.query(sql, sqlWhereValues, (error, result) => {
          if(error)
            return cb ? cb(error) : undefined

          if(result.length > 0) {
            lastSelectResult = result
            self.next()
          }
          if(cb) cb(result)
        })
      }

      /**
       * Skip to next record in the current resultset
       * @return {boolean} Return true if next record was loaded
       */
      let nextRecord = function() {
        if(!lastSelectResult)
          return
        if(!util.isArray(lastSelectResult))
          return

        if(lastSelectResult.length == 0) {
          lastSelectResult == undefined
          return false
        }

        let record = lastSelectResult.shift()
        fields.forEach(fieldName => {
          if(record[fieldName])
            self[fieldName] = record[fieldName]
        })

        return true
      }

      Object.defineProperty(this, 'save', { value: saveRecord })
      Object.defineProperty(this, 'select', { value: selectRecords })
      Object.defineProperty(this, 'next', { value: nextRecord })
      Object.seal(this)
    }
  }
}

module.exports = Database
