const mysql = require('mysql')
const config = {
    host: '127.0.0.1',
    port: 3306,
    user: 'root',
    password: 'root',
    database: '1234',
    supportBigNumbers: true,
    debug: false
}

const pool = mysql.createPool(config)

// -----基本函数
const cancel = function () {
  return new Promise((resolve, reject) => {
    this.conn.rollback(resolve)
  })
}
const close = function () {
  return new Promise((resolve, reject) => {
    this.conn.commit(async (err) => {
      if (err) {
        await cancel.bind({conn: this.conn})()
        this.conn.release() // 返还连接池
        return reject(err)
      }
      this.conn.release() // 返还连接池
      resolve()
    })
  })
}
const query = async function (sql, param) {
  let err = null
  let rows = []
  try {
    rows = await new Promise((resolve, reject) => {
      this.conn.query(sql, param || [], (err, result, fiedls) => {
        // result, 查询结果， fields， 返回字段
        if (err) return reject(err)
        resolve(result)
      })
    })
  } catch (e) {
    if (this.conn.cancel) {
      this.conn.cancel()
    }
    console.log(`-------sql err-------`, e.name, e.message)
    err = {
      err: 2,
      msg: '数据库错误'  // 一般是 sql 出错
    }
  }
  return [err, rows]
}
const getConn = () => {
  return new Promise((resolve, reject) => {
    pool.getConnection((err, connection) => {
      if (err) return reject(err)
      resolve(connection)
    })
  })
}

// -----关键词处理函数
let isStrInt = (v) => v !== null && v !== undefined && (typeof v === 'string' || typeof v === 'number')
let isNumber = (v) => !isNaN(parseInt(v))
let isStr = (v) => typeof v === 'string' && v !== ''
let isArray = (v) => Array.isArray(v) && v.length > 1
let isObject = (v) => typeof v === 'object' && !isArray(v)
let escape = mysql.escape // 防止注入
let arrToStr = (v) => `(${v.map((v1) => escape(v1)).join(',')})`

// {$limit: [0,1]}, {$limit: 1}
let isLimit = (v) => isArray(v) || isNumber(v)
let toLimit = (v) => {
  if (isArray(v)) {
    return ` limit ${v.map((v1) => {
      if (isNaN(parseInt(v1))) throw new Error('sql: limit err')
      return v1
    }).join(',')} `
  } else if (!isNaN(parseInt(v))) {
    return ` limit ${v} `
  } else {
    throw new Error('sql: limit err')
  }
}
// {$sort:{a:-1}}
let toSort = (v) => {
  let arr = []
  for (let v1 in v) {
    arr.push(v1, v[v1])
  }
  if (arr.length !== 2) throw new Error('sql: sort err')

  let sql = ` order by ${arr[0]}`
  if (arr[1] === -1) {
    sql += ' desc '
  } else if (arr[1] === 1) {
    sql += ' asc '
  } else {
    throw new Error('sql: sort val err')
  }
  return sql
}

// -----关键词

// $and: [] // 多个操作数
let arrOp = {
  $and: ['and', isArray],
  $or: ['or', isArray]
}
// 单个，not (a > 1), xx is null
let oneOp = {
  $isNull: ['is null', isStr, (k) => `${k} is null`],
  $not: ['not', isObject]
}

// a:{$in: [1,2,3],{$isNull:1}} // 判断取值
// 键， 值
let twoOp = {
  $in: ['in', isArray, arrToStr],
  $nin: ['not in', isArray, arrToStr],
  $regex: ['regexp', isStr],
  $like: ['like', isStr],
  $gt: ['>', isStrInt], // TODO 时间大小判断
  $gte: ['>=', isStrInt],
  $lt: ['<', isStrInt],
  $lte: ['<=', isStrInt],
  $ne: ['!=', isStrInt]
}

let groupOp = {
  $limit: ['limit', isLimit, toLimit],
  $sort: ['order by', isObject, toSort]
}

let sqlDg = (obj) => { // 先假设最后一层，{a:1,$or:[{a:1}],a:{$op:1}} // $or 的需要递归处理
  let sqlStr = []
  for (let key in obj) {
    let val = obj[key]
    if (val === null || val === undefined) throw new Error('sql val: err')

    let arrk = arrOp[key]
    let onek = oneOp[key]
    if (arrk !== undefined) {
      // 递归遍历 $or, $and 的值

      if (!arrk[1](val)) {
        throw new Error('arr_op: err val')
      }
      let sqlArr = []
      for (let or1 of val) {
        sqlArr.push(sqlDg(or1))
      }
      sqlStr.push(`(${sqlArr.join(` ${arrk[0]} `)})`)
    } else if (onek !== undefined) {
      // 单个操作值的判断

      if (!onek[1](val)) {
        throw new Error('one_op: err val')
      }
      // isNull, not, limit

      let sqlOne = `${onek[0]} ${typeof onek[2] === 'function' ? onek[2](val) : sqlDg(val)}`
      sqlStr.push(sqlOne)
    } else {
      // = 或 特殊操作符

      if (isStrInt(val)) {
        // 这是等于判断

        sqlStr.push(` ${key}=${escape(val)} `)
      } else if (isObject(val)) {
        // 这是 操作数判断

        let arrOp = []
        for (let twoObj in val) {
          arrOp.push(twoObj, val[twoObj])
        }
        if (arrOp.length !== 2) {
          throw new Error('once_op: err obj')
        }

        let twok = twoOp[arrOp[0]]
        if (twok === undefined || !twok[1](arrOp[1])) {
          throw new Error('once_op: err obj')
        }
        sqlStr.push(` ${key} ${twok[0]} ${typeof twok[2] === 'function' ? twok[2](arrOp[1]) : escape(arrOp[1])} `)
      } else {
        // 取值，错误
        throw new Error('once_op: err val')
      }
    }
  }
  return `(${sqlStr.join(' and ')})`
}

// -----基本操作函数
const find = function (table = '', query = {}, options = '') {
  let sql = 'select '
  if (typeof options === 'string' && options !== '') {
    sql += options.split(' ').join(',')
  } else {
    sql += '*'
  }

  sql += ` from ${table}`

  if (Object.keys(query).length > 0) {
    sql += ` where ${sqlDg(query)}`
  }
  return sql
}
const update = function (table = '', query = {}, option = {}) {
  if (!isObject(option)) throw new Error('find: table err')
  let up = []
  for (let o1 in option) {
    up.push(`${o1}=${option[o1]}`)
  }
  let str = ''
  if (up.length !== 0) {
    str = `set ${up.join(',')}`
  }
  let sql = `update ${table} ${str} where ${sqlDg(query)}`
  return sql
}
const remove = function (table = '', query = {}, options = {}) {
  let sql = `delete from ${table} where ${sqlDg(query)}`
  return sql
}
const insert = function (table = '', query = {}, options = {}) {
  let sql = `insert into ${table}`
  let key = []
  let val = []
  for (let o1 in options) {
    key.push(o1)
    val.push(escape(options[o1]))
  }
  sql += `(${key.join(',')})values(${val.join(',')})`
  return sql
}
const func = {find, update, insert, remove}

// -----方法注入
const register = (obj, func, query) => {
  for (let f1 in func) {
    let method = function (table, query, options, filter) {
      return new Promise(async (resolve, reject) => {
        if (!table || table === '') return reject(new Error('not table name'))
        let sql = ''
        try {
          sql = func[f1](table, query, options)
        } catch (e) {
          reject(e)
        }

        if (isObject(filter)) { // limit, ordery by
          let karr = []
          let gSql = ''
          for (let f1 in filter) {
            karr.push(f1)
            let groupk = groupOp[f1]
            let val = filter[f1]
            if (groupk === undefined || !groupk[1](val)) return reject(new Error('group op: err key'))
            gSql += groupk[2](val)
          }
          if (karr.length === 2) {
            if (karr[0] === '$limit') return reject(new Error('group op: err obj'))
          }
          sql += gSql
        }
        console.log('\n sql: ', sql, '\n')
        let val = await this.query(sql) // 是否会发生内存泄漏
        resolve(val)
      })
    }
    obj[f1] = method.bind(query)
  }
}

// -----构造类
class db {
  static isNew (obj) {
    return obj.affectedRows > 0
  }
  static async aQuery (sql, param) {
    let conn = await getConn()
    let v = query.bind({conn: conn})(sql, param)
    conn.release() // 返回连接池
    return v
  }
  static async beginSession () { // 开启事务的连接
    let conn = await getConn()
    conn.close = close.bind({conn})
    conn.cancel = cancel.bind({conn})
    let query1 = query.bind({conn})
    conn.aQuery = query1
    register(conn, func, {query: conn.aQuery})
    return new Promise((resolve, reject) => {
      conn.beginTransaction((err) => {
        if (err) return reject(err)
        resolve(conn)
      })
    })
  }
}
register(db, func, {query: db.aQuery})

module.exports = db

// -----test
void (async () => {

 // let [err, rows] = await db.aQuery(`select * from yh_ad`)
  // console.log(rows[0].id)
  let session = await db.beginSession()
  let [r1, r2] = await session.find('yh_ad', {}, '', {
    $sort: {id: -1}
  })
  console.log(r1, r2)

 //  console.log(v[1].id
 //  )
 // let v = find('user', 'a=1 and b=2', 'b,c')
 // let v = update('user', 'a=1 and b=1', {a:1,b:2})
 //  let v = update('user', {
 //    $and: [
 //      {a: 1},
 //      {$not: {
 //        $isNull: 'a',
 //        a: {$gt: 1}
 //      }}
 //    ]
 //  }, {
 //    a: 1
 //  })
 // let v = insert('user', {a:1,b:1})
})()