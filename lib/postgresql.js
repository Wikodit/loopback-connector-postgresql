// Copyright IBM Corp. 2013,2020. All Rights Reserved.
// Node module: loopback-connector-postgresql
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

/*!
 * PostgreSQL connector for LoopBack
 */
'use strict';
const SG = require('strong-globalize');
const g = SG();
const postgresql = require('pg');
const SqlConnector = require('loopback-connector').SqlConnector;
const ParameterizedSQL = SqlConnector.ParameterizedSQL;
const util = require('util');
const debug = require('debug')('loopback:connector:postgresql');
const debugData = require('debug')('loopback:connector:postgresql:data');
const debugSort = require('debug')('loopback:connector:postgresql:order');
const Promise = require('bluebird');

/**
 *
 * Initialize the PostgreSQL connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @callback {Function} [callback] The callback function
 * @param {String|Error} err The error string or object
 * @header PostgreSQL.initialize(dataSource, [callback])
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  if (!postgresql) {
    return;
  }

  const dbSettings = dataSource.settings || {};
  dbSettings.host = dbSettings.host || dbSettings.hostname || 'localhost';
  dbSettings.user = dbSettings.user || dbSettings.username;

  dataSource.connector = new PostgreSQL(postgresql, dbSettings);
  dataSource.connector.dataSource = dataSource;

  if (callback) {
    if (dbSettings.lazyConnect) {
      process.nextTick(callback);
    } else {
      dataSource.connecting = true;
      dataSource.connector.connect(callback);
    }
  }
};

/**
 * PostgreSQL connector constructor
 *
 * @param {PostgreSQL} postgresql PostgreSQL node.js binding
 * @options {Object} settings An object for the data source settings.
 * See [node-postgres documentation](https://github.com/brianc/node-postgres/wiki/Client#parameters).
 * @property {String} url URL to the database, such as 'postgres://test:mypassword@localhost:5432/devdb'.
 * Other parameters can be defined as query string of the url
 * @property {String} hostname The host name or ip address of the PostgreSQL DB server
 * @property {Number} port The port number of the PostgreSQL DB Server
 * @property {String} user The user name
 * @property {String} password The password
 * @property {String} database The database name
 * @property {Boolean} ssl Whether to try SSL/TLS to connect to server
 *
 * @constructor
 */
function PostgreSQL(postgresql, settings) {
  // this.name = 'postgresql';
  // this._models = {};
  // this.settings = settings;
  this.constructor.super_.call(this, 'postgresql', settings);
  this.clientConfig = settings;
  if (settings.url) {
    // pg-pool doesn't handle string config correctly
    this.clientConfig.connectionString = settings.url;
  }
  this.clientConfig.Promise = Promise;
  this.pg = new postgresql.Pool(this.clientConfig);

  this.settings = settings;
  debug('Settings %j', settings);
}

// Inherit from loopback-datasource-juggler BaseSQL
util.inherits(PostgreSQL, SqlConnector);

PostgreSQL.prototype.getDefaultSchemaName = function() {
  return 'public';
};

/**
 * Connect to PostgreSQL
 * @callback {Function} [callback] The callback after the connection is established
 */
PostgreSQL.prototype.connect = function(callback) {
  const self = this;
  self.pg.connect(function(err, client, releaseCb) {
    self.client = client;
    process.nextTick(releaseCb);
    callback && callback(err, client);
  });
};

PostgreSQL.prototype.count = function(model, where, options, cb) {
  const self = this;
  if (typeof where === 'function') {
    // Backward compatibility for 1.x style signature:
    // count(model, cb, where)
    const tmp = options;
    cb = where;
    where = tmp;
  }

  const stmt = self.buildSelect(model, {
    where,
  }, options, true);
  self.execute(stmt.sql, stmt.params, options,
    function(err, res) {
      if (err) {
        return cb(err);
      }
      const c = (res && res[0] && res[0].cnt) || 0;
      // Some drivers return count as a string to contain bigint
      // See https://github.com/brianc/node-postgres/pull/427
      cb(err, Number(c));
    });
};

/**
 * Execute the sql statement
 *
 * @param {String} sql The SQL statement
 * @param {String[]} params The parameter values for the SQL statement
 * @param {Object} [options] Options object
 * @callback {Function} [callback] The callback after the SQL statement is executed
 * @param {String|Error} err The error string or object
 * @param {Object[]} data The result from the SQL
 */
PostgreSQL.prototype.executeSQL = function(sql, params, options, callback) {
  const self = this;

  if (params && params.length > 0) {
    debug('SQL: %s\nParameters: %j', sql, params);
  } else {
    debug('SQL: %s', sql);
  }

  function executeWithConnection(connection, releaseCb) {
    connection.query(sql, params, function(err, data) {
      // if(err) console.error(err);
      if (err) debug(err);
      if (data) debugData('%j', data);
      // Release the connection back to the pool.
      if (releaseCb) releaseCb(err);
      let result = null;
      if (data) {
        switch (data.command) {
          case 'DELETE':
          case 'UPDATE':
            result = {
              affectedRows: data.rowCount,
              count: data.rowCount,
            };

            if (data.rows) result.rows = data.rows;

            break;
          default:
            result = data.rows;
        }
      }
      callback(err ? err : null, result);
    });
  }

  const transaction = options.transaction;
  if (transaction && transaction.connector === this) {
    if (!transaction.connection) {
      return process.nextTick(function() {
        callback(new Error(g.f('Connection does not exist')));
      });
    }
    if (transaction.txId !== transaction.connection.txId) {
      return process.nextTick(function() {
        callback(new Error(g.f('Transaction is not active')));
      });
    }
    debug('Execute SQL within a transaction');
    // Do not release the connection
    executeWithConnection(transaction.connection, null);
  } else {
    self.pg.connect(function(err, connection, releaseCb) {
      if (err) return callback(err);
      executeWithConnection(connection, releaseCb);
    });
  }
};

PostgreSQL.prototype.buildInsertReturning = function(model, data, options) {
  const idColumnNames = [];
  const idNames = this.idNames(model);
  for (let i = 0, n = idNames.length; i < n; i++) {
    idColumnNames.push(this.columnEscaped(model, idNames[i]));
  }
  return 'RETURNING ' + idColumnNames.join(',');
};

/**
 * Check if id types have a numeric type
 * @param {String} model name
 * @returns {Boolean}
 */
PostgreSQL.prototype.hasOnlyNumericIds = function(model) {
  const cols = this.getModelDefinition(model).properties;
  const idNames = this.idNames(model);
  const numericIds = idNames.filter(function(idName) {
    return cols[idName].type === Number;
  });

  return numericIds.length == idNames.length;
};

/**
 * Get default find sort policy
 * @param model
 */
PostgreSQL.prototype.getDefaultIdSortPolicy = function(model) {
  const modelClass = this._models[model];

  if (modelClass.settings.hasOwnProperty('defaultIdSort')) {
    return modelClass.settings.defaultIdSort;
  }

  if (this.settings.hasOwnProperty('defaultIdSort')) {
    return this.settings.defaultIdSort;
  }

  return null;
};

/**
 * Build a list of escaped column names for the given model and fields filter
 * @param {string} model Model name
 * @param {object} filter The filter object
 * @param {boolean} withTable If true prepend the table name (default false)
 * @returns {string} Comma separated string of escaped column names
 */
PostgreSQL.prototype.buildColumnNames = function(model, filter, withTable = false) {
  const fieldsFilter = filter && filter.fields;
  const cols = this.getModelDefinition(model).properties;
  if (!cols) {
    return '*';
  }
  const self = this;
  let keys = Object.keys(cols);
  if (Array.isArray(fieldsFilter) && fieldsFilter.length > 0) {
    // Not empty array, including all the fields that are valid properties
    keys = fieldsFilter.filter(function(f) {
      return cols[f];
    });
  } else if ('object' === typeof fieldsFilter &&
    Object.keys(fieldsFilter).length > 0) {
    // { field1: boolean, field2: boolean ... }
    const included = [];
    const excluded = [];
    keys.forEach(function(k) {
      if (fieldsFilter[k]) {
        included.push(k);
      } else if ((k in fieldsFilter) && !fieldsFilter[k]) {
        excluded.push(k);
      }
    });
    if (included.length > 0) {
      keys = included;
    } else if (excluded.length > 0) {
      excluded.forEach(function(e) {
        const index = keys.indexOf(e);
        keys.splice(index, 1);
      });
    }
  }
  const names = keys.map(function(c) {
    return self.columnEscaped(model, c, withTable);
  });
  return names.join(',');
};

/**
 * Build a SQL SELECT statement
 * @param {String} model Model name
 * @param {Object} filter Filter object
 * @param {Object} options Options object
 * @returns {ParameterizedSQL} Statement object {sql: ..., params: ...}
 */
PostgreSQL.prototype.buildSelect = function(model, filter, options = {}, count = false) {
  let sortById;

  const sortPolicy = this.getDefaultIdSortPolicy(model);

  switch (sortPolicy) {
    case 'numericIdOnly':
      sortById = this.hasOnlyNumericIds(model);
      break;
    case false:
      sortById = false;
      break;
    default:
      sortById = true;
      break;
  }

  debugSort(model, 'sort policy:', sortPolicy, sortById);

  if (sortById && !filter.order && !count) {
    const idNames = this.idNames(model);
    if (idNames && idNames.length) {
      filter.order = idNames;
    }
  }

  const selectSql = count ?
    'count(' + this.tableEscaped(model) + '.' + this.idNames(model) + ') as "cnt"' :
    this.buildColumnNames(model, filter, true);

  let selectStmt = new ParameterizedSQL(
    'SELECT ' +
    selectSql +
    ' FROM ' +
    this.tableEscaped(model),
  );

  if (filter) {
    if (filter.where || filter.order) {
      const {
        joinStmt,
        innerWhere,
        innerOrder,
      } = this.buildJoin(
        model,
        filter,
      );
      filter.innerWhere = innerWhere || {};
      filter.innerOrder = innerOrder || {};
      selectStmt.merge(joinStmt);
    }
    if (filter.where) {
      const whereStmt = this.buildWhere(model, filter);
      selectStmt.merge(whereStmt);
    }

    if (filter.order) {
      selectStmt.merge(this.buildOrderBy(model, filter));
    }

    if (filter.limit || filter.skip || filter.offset) {
      selectStmt = this.applyPagination(model, selectStmt, filter);
    }
  }
  return this.parameterize(selectStmt);
};

/**
 * Build the ORDER BY clause
 * @param {string} model Model name
 * @param {string[]} order An array of sorting criteria
 * @returns {string} The ORDER BY clause
 */
PostgreSQL.prototype.buildOrderBy = function(model, filter) {
  const {
    order,
    innerOrder,
  } = filter;
  if (!order) {
    return '';
  }
  const self = this;
  let orderArray = order;
  if (typeof order === 'string') {
    orderArray = [order];
  }
  const clauses = [];
  for (let i = 0, n = orderArray.length; i < n; i++) {
    const t = orderArray[i].split(/[\s,]+/);
    let value = false;
    if (t.length === 2) {
      value = true;
    }
    if (innerOrder && innerOrder[t[0]]) {
      const column = self.columnEscaped(
        innerOrder[t[0]].model,
        innerOrder[t[0]].property.key,
        true,
        innerOrder[t[0]].prefix,
      );
      if (value) {
        clauses.push(column + ' ' + t[1]);
      } else {
        clauses.push(column);
      }
    } else {
      const column = self.columnEscaped(model, t[0], true);
      if (value) {
        clauses.push(column + ' ' + t[1]);
      } else {
        clauses.push(column);
      }
    }
  }
  return 'ORDER BY ' + clauses.join(',');
};

PostgreSQL.prototype.buildInsertDefaultValues = function(
  model,
  data,
  options,
) {
  return 'DEFAULT VALUES';
};

// FIXME: [rfeng] The native implementation of upsert only works with
// postgresql 9.1 or later as it requres writable CTE
// See https://github.com/strongloop/loopback-connector-postgresql/issues/27
/**
 * Update if the model instance exists with the same id or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @callback {Function} [callback] The callback function
 * @param {String|Error} err The error string or object
 * @param {Object} The updated model instance
 */
/*
 PostgreSQL.prototype.updateOrCreate = function (model, data, callback) {
 var self = this;
 data = self.mapToDB(model, data);
 var props = self._categorizeProperties(model, data);
 var idColumns = props.ids.map(function(key) {
 return self.columnEscaped(model, key); }
 );
 var nonIdsInData = props.nonIdsInData;
 var query = [];
 query.push('WITH update_outcome AS (UPDATE ', self.tableEscaped(model), ' SET ');
 query.push(self.toFields(model, data, false));
 query.push(' WHERE ');
 query.push(idColumns.map(function (key, i) {
 return ((i > 0) ? ' AND ' : ' ') + key + '=$' + (nonIdsInData.length + i + 1);
 }).join(','));
 query.push(' RETURNING ', idColumns.join(','), ')');
 query.push(', insert_outcome AS (INSERT INTO ', self.tableEscaped(model), ' ');
 query.push(self.toFields(model, data, true));
 query.push(' WHERE NOT EXISTS (SELECT * FROM update_outcome) RETURNING ', idColumns.join(','), ')');
 query.push(' SELECT * FROM update_outcome UNION ALL SELECT * FROM insert_outcome');
 var queryParams = [];
 nonIdsInData.forEach(function(key) {
 queryParams.push(data[key]);
 });
 props.ids.forEach(function(key) {
 queryParams.push(data[key] || null);
 });
 var idColName = self.idColumn(model);
 self.query(query.join(''), queryParams, function(err, info) {
 if (err) {
 return callback(err);
 }
 var idValue = null;
 if (info && info[0]) {
 idValue = info[0][idColName];
 }
 callback(err, idValue);
 });
 };
 */

PostgreSQL.prototype.fromColumnValue = function(prop, val) {
  if (val == null) {
    return val;
  }
  const type = prop.type && prop.type.name;
  if (prop && type === 'Boolean') {
    if (typeof val === 'boolean') {
      return val;
    } else {
      return (
        val === 'Y' || val === 'y' || val === 'T' || val === 't' || val === '1'
      );
    }
  } else if ((prop && type === 'GeoPoint') || type === 'Point') {
    if (typeof val === 'string') {
      // The point format is (x,y)
      const point = val.split(/[\(\)\s,]+/).filter(Boolean);
      return {
        lat: +point[0],
        lng: +point[1],
      };
    } else if (typeof val === 'object' && val !== null) {
      // Now pg driver converts point to {x: lng, y: lat}
      return {
        lng: val.x,
        lat: val.y,
      };
    } else {
      return val;
    }
  } else {
    return val;
  }
};

/*!
 * Convert to the Database name
 * @param {String} name The name
 * @returns {String} The converted name
 */
PostgreSQL.prototype.dbName = function(name) {
  if (!name) {
    return name;
  }
  // PostgreSQL default to lowercase names
  return name.toLowerCase();
};

function escapeIdentifier(str) {
  let escaped = '"';
  for (let i = 0; i < str.length; i++) {
    const c = str[i];
    if (c === '"') {
      escaped += c + c;
    } else {
      escaped += c;
    }
  }
  escaped += '"';
  return escaped;
}

function escapeLiteral(str) {
  let hasBackslash = false;
  let escaped = "'";
  for (let i = 0; i < str.length; i++) {
    const c = str[i];
    if (c === "'") {
      escaped += c + c;
    } else if (c === '\\') {
      escaped += c + c;
      hasBackslash = true;
    } else {
      escaped += c;
    }
  }
  escaped += "'";
  if (hasBackslash === true) {
    escaped = ' E' + escaped;
  }
  return escaped;
}

/*
 * Check if a value is attempting to use nested json keys
 * @param {String} property The property being queried from where clause
 * @returns {Boolean} True of the property contains dots for nested json
 */
function isNested(property) {
  return property.split('.').length > 1;
}

/*
 * Overwrite the loopback-connector column escape
 * to allow querying nested json keys
 * @param {String} model The model name
 * @param {String} property The property name
 * @param {boolean} if true prepend the table name (default= false)
 * @param {String} add a prefix to column (for alias)
 * @returns {String} The escaped column name, or column with nested keys for deep json columns
 */
PostgreSQL.prototype.columnEscaped = function(model, property, withTable = false, prefix = '') {
  if (isNested(property)) {
    // Convert column to PostgreSQL json style query: "model"->>'val'
    const self = this;
    return property
      .split('.')
      .map(function(val, idx) {
        return idx === 0 ? self.columnEscaped(model, val) : escapeLiteral(val);
      })
      .reduce(function(prev, next, idx, arr) {
        return idx == 0 ?
          next :
          idx < arr.length - 1 ?
            prev + '->' + next :
            prev + '->>' + next;
      });
  } else {
    if (withTable) {
      return (
        this.escapeName(prefix + this.table(model)) +
        '.' +
        this.escapeName(this.column(model, property))
      );
    } else {
      return this.escapeName(this.column(model, property));
    }
  }
};

/*!
 * Escape the name for PostgreSQL DB
 * @param {String} name The name
 * @returns {String} The escaped name
 */
PostgreSQL.prototype.escapeName = function(name) {
  if (!name) {
    return name;
  }
  return escapeIdentifier(name);
};

PostgreSQL.prototype.escapeValue = function(value) {
  if (typeof value === 'string') {
    return escapeLiteral(value);
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }
  // Can't send functions, objects, arrays
  if (typeof value === 'object' || typeof value === 'function') {
    return null;
  }
  return value;
};

PostgreSQL.prototype.tableEscaped = function(model) {
  const schema = this.schema(model) || 'public';
  return this.escapeName(schema) + '.' + this.escapeName(this.table(model));
};

function buildLimit(limit, offset) {
  const clause = [];
  if (isNaN(limit)) {
    limit = 0;
  }
  if (isNaN(offset)) {
    offset = 0;
  }
  if (!limit && !offset) {
    return '';
  }
  if (limit) {
    clause.push('LIMIT ' + limit);
  }
  if (offset) {
    clause.push('OFFSET ' + offset);
  }
  return clause.join(' ');
}

PostgreSQL.prototype.applyPagination = function(model, stmt, filter) {
  const limitClause = buildLimit(filter.limit, filter.offset || filter.skip);
  return stmt.merge(limitClause);
};

PostgreSQL.prototype.buildExpression = function(
  columnName,
  operator,
  operatorValue,
  propertyDefinition,
) {
  switch (operator) {
    case 'like':
      return new ParameterizedSQL(columnName + "::TEXT LIKE ? ESCAPE E'\\\\'", [
        operatorValue,
      ]);
    case 'ilike':
      return new ParameterizedSQL(
        columnName + "::TEXT ILIKE ? ESCAPE E'\\\\'",
        [operatorValue],
      );
    case 'nlike':
      return new ParameterizedSQL(
        columnName + "::TEXT NOT LIKE ? ESCAPE E'\\\\'",
        [operatorValue],
      );
    case 'nilike':
      return new ParameterizedSQL(
        columnName + "::TEXT NOT ILIKE ? ESCAPE E'\\\\'",
        [operatorValue],
      );
    case 'regexp':
      if (operatorValue.global)
        g.warn('{{PostgreSQL}} regex syntax does not respect the {{`g`}} flag');

      if (operatorValue.multiline)
        g.warn('{{PostgreSQL}} regex syntax does not respect the {{`m`}} flag');

      const regexOperator = operatorValue.ignoreCase ? ' ~* ?' : ' ~ ?';
      return new ParameterizedSQL(columnName + regexOperator, [
        operatorValue.source,
      ]);
    default:
      // invoke the base implementation of `buildExpression`
      return this.invokeSuper(
        'buildExpression',
        columnName,
        operator,
        operatorValue,
        propertyDefinition,
      );
  }
};

/**
 * Disconnect from PostgreSQL
 * @param {Function} [cb] The callback function
 */
PostgreSQL.prototype.disconnect = function disconnect(cb) {
  if (this.pg) {
    debug('Disconnecting from ' + this.settings.hostname);
    const pg = this.pg;
    this.pg = null;
    pg.end(); // This is sync
  }

  if (cb) {
    process.nextTick(cb);
  }
};

PostgreSQL.prototype.ping = function(cb) {
  this.execute('SELECT 1 AS result', [], cb);
};

PostgreSQL.prototype.getInsertedId = function(model, info) {
  const idColName = this.idColumn(model);
  let idValue;
  if (info && info[0]) {
    idValue = info[0][idColName];
  }
  return idValue;
};

/**
 * Build the INNER JOIN AS clauses for the filter.where object
 * Prepare WHERE clause with the creation innerWhere object
 * Prepare ORDER BY clause with the creation of innerOrder object
 * @param {string} model Model name
 * @param {object} filter An object for the filter
 * @returns {object} An object with the INNER JOIN AS SQL statement innerWhere and innerOrder for next builders
 */
PostgreSQL.prototype.buildJoin = function(model, filter) {
  const {
    order,
    where,
  } = filter;
  let candidateProperty, candidateRelations;
  const innerJoins = [];
  const innerOrder = {};
  const innerWhere = {};
  if (!where && !order) {
    return new ParameterizedSQL('');
  }
  if ((typeof where !== 'object' || Array.isArray(where)) && !order) {
    debug('Invalid value for where: %j', where);
    return new ParameterizedSQL('');
  }
  const self = this;
  const props = self.getModelDefinition(model).properties;
  let basePrefix = 0;

  const _buildJoin = (key, type) => {
    let orderValue = '';
    if (type === 'order') {
      const t = key.split(/[\s,]+/);
      orderValue = t.length === 1 ? '' : t[1];
      key = t.length === 1 ? key : t[0];
    }
    let p = props[key];
    if (p) {
      return;
    }
    if (p == null && isNested(key)) {
      p = props[key.split('.')[0]];
    }
    if (p) {
      return;
    }
    // It may be an innerWhere
    const splitedKey = key.split('.');
    if (splitedKey.length === 1) {
      debug('Unknown property %s is skipped for model %s', key, model);
      return;
    }
    // Pop the property
    candidateProperty = splitedKey.pop();
    // Keep the candidate relations
    candidateRelations = splitedKey;
    let parentModel = model;
    let parentPrefix = '';
    for (let i = 0; i < candidateRelations.length; i++) {
      // Build a prefix for alias to prevent conflict
      const prefix = `_${basePrefix}_${i}_`;
      const candidateRelation = candidateRelations[i];
      const modelDefinition = this.getModelDefinition(parentModel);

      if (!modelDefinition) {
        debug('No definition for model %s', parentModel);
        break;
      }
      // The next line need a monkey patch of @loopback/repository to add relations to modelDefinition */
      if (!(candidateRelation in modelDefinition.settings.__relations__)) {
        debug('No relation for model %s', parentModel);
        break;
      }

      if (Array.isArray(modelDefinition.settings.__hiddenRelations__) &&
        modelDefinition.settings.__hiddenRelations__.includes(candidateRelation)) {
        debug('Hidden relation for model %s skipping', parentModel);
        break;
      }
      const relation =
        modelDefinition.settings.__relations__[candidateRelation];
      // Only supports belongsTo, hasOne and hasMany
      if (relation.type !== 'belongsTo' && relation.type !== 'hasOne' && relation.type !== 'hasMany') {
        debug('Invalid relation type for model %s for inner join', parentModel);
        break;
      }

      const target = relation.target();
      const targetDefinition = this.getModelDefinition(target.modelName);
      let hasProp = true;
      if (!(candidateProperty in targetDefinition.properties)) {
        if (targetDefinition.settings.__relations__[candidateRelations[i + 1]]) {
          hasProp = false;
        } else {
          debug(
            'Unknown property %s is skipped for model %s',
            candidateProperty,
            target.modelName,
          );
          break;
        }
      }

      // Check if join already done
      let alreadyJoined = false;
      for (const innerJoin of innerJoins) {
        if (
          innerJoin.model === target.modelName &&
          innerJoin.parentModel === parentModel
        ) {
          alreadyJoined = true;
          // Keep what needed to build the WHERE or ORDER statement properly
          if (type === 'where' && hasProp) {
            innerWhere[key] = {
              prefix: innerJoin.prefix,
              model: innerJoin.model,
              property: {
                ...targetDefinition.properties[candidateProperty],
                key: candidateProperty,
              },
              value: where[key],
            };
          } else if (type === 'order' && hasProp) {
            innerOrder[key] = {
              prefix: innerJoin.prefix,
              model: innerJoin.model,
              property: {
                ...targetDefinition.properties[candidateProperty],
                key: candidateProperty,
              },
              value: orderValue,
            };
          }
        }
      }
      // Keep what needed to build INNER JOIN AS statement
      if (!alreadyJoined) {
        innerJoins.push({
          prefix,
          parentPrefix,
          parentModel,
          relation: {
            ...relation,
            name: candidateRelation,
          },
          model: target.modelName,
        });
        // Keep what needed to build the WHERE or ORDER statement properly
        if (type === 'where' && hasProp) {
          innerWhere[key] = {
            prefix,
            model: target.modelName,
            property: {
              ...targetDefinition.properties[candidateProperty],
              key: candidateProperty,
            },
            value: where[key],
          };
        } else if (type === 'order' && hasProp) {
          innerOrder[key] = {
            prefix,
            model: target.modelName,
            property: {
              ...targetDefinition.properties[candidateProperty],
              key: candidateProperty,
            },
            value: orderValue,
          };
        }
      }
      // Keep the parentModel for recursive INNER JOIN and parentPrefix for the alias
      parentModel = target.modelName;
      parentPrefix = prefix;
    }
    basePrefix++;
  };

  const _extractKeys = (where, _keys = new Set()) => {
    for (const key in where) {
      if (key !== 'or' && key !== 'and') {
        _keys.add(key);
        continue;
      }

      const clauses = where[key];
      if (Array.isArray(clauses)) {
        for (const clause of clauses) {
          _extractKeys(clause, _keys);
        }
      }
    }

    return _keys;
  };

  const flatWhere = [..._extractKeys(where)];

  for (const key of flatWhere) {
    _buildJoin(key, 'where');
  }
  let orderArray = order;
  if (typeof order === 'string') {
    orderArray = [order];
  }
  if (Array.isArray(orderArray)) {
    for (const key of orderArray) {
      _buildJoin(key, 'order');
    }
  }

  const joinStmt = {
    sql: '',
  };

  for (const innerJoin of innerJoins) {
    joinStmt.sql += `INNER JOIN ${self.tableEscaped(
      innerJoin.model,
    )} AS ${self.escapeName(innerJoin.prefix + self.table(innerJoin.model))} `;
    /** @todo Fix ::uuid to be dynamic */
    joinStmt.sql += `ON ${self.columnEscaped(
      innerJoin.parentModel,
      innerJoin.relation.keyFrom,
      true,
      innerJoin.parentPrefix,
    )}${
      innerJoin.relation.type === 'belongsTo' ? '::uuid' : ''
    } = ${self.columnEscaped(
      innerJoin.model,
      innerJoin.relation.keyTo,
      true,
      innerJoin.prefix,
    )}${innerJoin.relation.type === 'hasOne'
      || innerJoin.relation.type === 'hasMany' ? '::uuid' : ''} `;
  }

  return {
    joinStmt,
    innerWhere,
    innerOrder,
  };
};

/**
 * Build the SQL WHERE clause for the filter object
 * @param {string} model Model name
 * @param {object} filter An object for the filter conditions
 * @returns {ParameterizedSQL} The SQL WHERE clause
 */
PostgreSQL.prototype.buildWhere = function(model, filter) {
  const whereClause = this._buildWhere(model, filter);
  if (whereClause.sql) {
    whereClause.sql = 'WHERE ' + whereClause.sql;
  }
  return whereClause;
};

/**
 * @private
 * @param model
 * @param filter
 * @returns {ParameterizedSQL}
 */
PostgreSQL.prototype._buildWhere = function(model, filter) {
  const {
    innerWhere,
  } = filter;
  let {
    where,
  } = filter;
  let columnValue, sqlExp;
  let withTable = true;
  if (!where && !innerWhere) {
    withTable = false;
    where = filter;
  }
  if (!filter) {
    return new ParameterizedSQL('');
  }
  if (typeof where !== 'object' || Array.isArray(where)) {
    debug('Invalid value for where: %j', where);
    return new ParameterizedSQL('');
  }
  const self = this;
  const props = self.getModelDefinition(model).properties;
  const modelDefinition = this.getModelDefinition(model);

  const whereStmts = [];
  for (const key in where) {
    let innerJoin = false;
    const stmt = new ParameterizedSQL('', []);
    // Handle and/or operators
    if (key === 'and' || key === 'or') {
      const branches = [];
      let branchParams = [];
      const clauses = where[key];
      if (Array.isArray(clauses)) {
        for (let i = 0, n = clauses.length; i < n; i++) {
          const stmtForClause = self._buildWhere(model, {
            innerWhere,
            where: clauses[i],
          });
          if (stmtForClause.sql) {
            stmtForClause.sql = '(' + stmtForClause.sql + ')';
            branchParams = branchParams.concat(stmtForClause.params);
            branches.push(stmtForClause.sql);
          }
        }
        stmt.merge({
          sql: branches.join(' ' + key.toUpperCase() + ' '),
          params: branchParams,
        });
        whereStmts.push(stmt);
        continue;
      }
      // The value is not an array, fall back to regular fields
    }
    let p = props[key];

    if (Array.isArray(modelDefinition.settings.__hiddenProps__) &&
      modelDefinition.settings.__hiddenProps__.includes(p)) {
      debug('Hidden prop for model %s skipping', model);
      continue;
    }

    if (p == null && isNested(key)) {
      // See if we are querying nested json
      p = props[key.split('.')[0]];
    }
    // It may be an innerWhere
    if (p == null) {
      if (innerWhere && innerWhere[key]) {
        p = innerWhere[key].property;
      }
      if (p) {
        innerJoin = true;
      }
    }

    if (p == null) {
      // Unknown property, ignore it
      debug('Unknown property %s is skipped for model %s', key, model);
      continue;
    }
    // eslint-disable one-var
    let expression = where[key];
    // Use alias from INNER JOIN AS builder
    const columnName = innerJoin ?
      self.columnEscaped(
        innerWhere[key].model,
        innerWhere[key].property.key,
        true,
        innerWhere[key].prefix,
      ) :
      self.columnEscaped(model, key, withTable);

    // eslint-enable one-var
    if (expression === null || expression === undefined) {
      stmt.merge(columnName + ' IS NULL');
    } else if (expression && expression.constructor === Object) {
      const operator = Object.keys(expression)[0];
      // Get the expression without the operator
      expression = expression[operator];
      if (operator === 'inq' || operator === 'nin' || operator === 'between') {
        columnValue = [];
        if (Array.isArray(expression)) {
          // Column value is a list
          for (let j = 0, m = expression.length; j < m; j++) {
            columnValue.push(this.toColumnValue(p, expression[j], true));
          }
        } else {
          columnValue.push(this.toColumnValue(p, expression, true));
        }
        if (operator === 'between') {
          // BETWEEN v1 AND v2
          const v1 = columnValue[0] === undefined ? null : columnValue[0];
          const v2 = columnValue[1] === undefined ? null : columnValue[1];
          columnValue = [v1, v2];
        } else {
          // IN (v1,v2,v3) or NOT IN (v1,v2,v3)
          if (columnValue.length === 0) {
            if (operator === 'inq') {
              columnValue = [null];
            } else {
              // nin () is true
              continue;
            }
          }
        }
      } else if (operator === 'regexp' && expression instanceof RegExp) {
        // do not coerce RegExp based on property definitions
        columnValue = expression;
      } else {
        columnValue = this.toColumnValue(p, expression, true);
      }
      sqlExp = self.buildExpression(columnName, operator, columnValue, p);
      stmt.merge(sqlExp);
    } else {
      // The expression is the field value, not a condition
      columnValue = self.toColumnValue(p, expression);
      if (columnValue === null) {
        stmt.merge(columnName + ' IS NULL');
      } else {
        if (columnValue instanceof ParameterizedSQL) {
          if (p.type.name === 'GeoPoint')
            stmt.merge(columnName + '~=').merge(columnValue);
          else stmt.merge(columnName + '=').merge(columnValue);
        } else {
          stmt.merge({
            sql: columnName + '=?',
            params: [columnValue],
          });
        }
      }
    }
    whereStmts.push(stmt);
  }
  let params = [];
  const sqls = [];
  for (let k = 0, s = whereStmts.length; k < s; k++) {
    if (whereStmts[k].sql) {
      sqls.push(whereStmts[k].sql);
      params = params.concat(whereStmts[k].params);
    }
  }
  const whereStmt = new ParameterizedSQL({
    sql: sqls.join(' AND '),
    params: params,
  });
  return whereStmt;
};

/*!
 * Convert property name/value to an escaped DB column value
 * @param {Object} prop Property descriptor
 * @param {*} val Property value
 * @param {boolean} isWhereClause
 * @returns {*} The escaped value of DB column
 */
PostgreSQL.prototype.toColumnValue = function(prop, val, isWhereClause) {
  if (val == null) {
    // PostgreSQL complains with NULLs in not null columns
    // If we have an autoincrement value, return DEFAULT instead
    // Do not return 'DEFAULT' for id field in where clause
    if (prop.autoIncrement || (prop.id && !isWhereClause)) {
      return new ParameterizedSQL('DEFAULT');
    } else {
      return null;
    }
  }
  if (prop.type === String) {
    return String(val);
  }
  if (prop.type === Number) {
    if (isNaN(val)) {
      // Map NaN to NULL
      return val;
    }
    return val;
  }

  if (prop.type === Date || prop.type.name === 'Timestamp') {
    if (!val.toISOString) {
      val = new Date(val);
    }
    const iso = val.toISOString();

    // Pass in date as UTC and make sure Postgresql stores using UTC timezone
    return new ParameterizedSQL({
      sql: '?::TIMESTAMP WITH TIME ZONE',
      params: [iso],
    });
  }

  // PostgreSQL support char(1) Y/N
  if (prop.type === Boolean) {
    if (val) {
      return true;
    } else {
      return false;
    }
  }

  if (prop.type.name === 'GeoPoint' || prop.type.name === 'Point') {
    return new ParameterizedSQL({
      sql: 'point(?,?)',
      // Postgres point is point(lng, lat)
      params: [val.lng, val.lat],
    });
  }

  if (Array.isArray(prop.type)) {
    // There is two possible cases for the type of "val" as well as two cases for dataType
    const isArrayDataType =
      prop.postgresql && prop.postgresql.dataType === 'varchar[]';
    if (Array.isArray(val)) {
      if (isArrayDataType) {
        return val;
      } else {
        return JSON.stringify(val);
      }
    } else {
      if (isArrayDataType) {
        return JSON.parse(val);
      } else {
        return val;
      }
    }
  }

  return val;
};

/**
 * Get the place holder in SQL for identifiers, such as ??
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
PostgreSQL.prototype.getPlaceholderForIdentifier = function(key) {
  throw new Error(g.f('{{Placeholder}} for identifiers is not supported'));
};

/**
 * Get the place holder in SQL for values, such as :1 or ?
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
PostgreSQL.prototype.getPlaceholderForValue = function(key) {
  return '$' + key;
};

PostgreSQL.prototype.getCountForAffectedRows = function(model, info) {
  return info && info.affectedRows;
};

require('./discovery')(PostgreSQL);
require('./migration')(PostgreSQL);
require('./transaction')(PostgreSQL);
