
/*!
 * kue - http - routes - json
 * Copyright (c) 2011 LearnBoost <tj@learnboost.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var Queue = require('../../kue')
  , Job = require('../../queue/job')
  , reds = require('reds')
  , queue = new Queue;

/**
 * Search instance.
 */

var search;
function getSearch() {
  if (search) return search;
  reds.createClient = require('../../redis').createClient;
  return search = reds.createSearch('q:search');
};

/**
 * Get statistics including:
 * 
 *   - delayed count
 *   - waiting count
 *   - staged count
 *   - inactive count
 *   - active count
 *   - complete count
 *   - failed count
 *
 */

exports.stats = function(req, res){
  get(queue)
    ('delayedCount')
    ('waitingCount')
    ('stagedCount')
    ('inactiveCount')
    ('completeCount')
    ('activeCount')
    ('failedCount')
    ('workTime')
    (function(err, obj){
      if (err) return safeSend(res, { error: err.message });
      safeSend(res, obj);
    });
};

/**
 * Get job types.
 */

exports.types = function(req, res){
  queue.types(function(err, types){
    if (err) return safeSend(res, { error: err.message });
    safeSend(res, types);
  });
};

/**
 * Get jobs by range :from..:to.
 */

exports.jobRange = function(req, res){
  var state = req.params.state
    , from = parseInt(req.params.from, 10)
    , to = parseInt(req.params.to, 10)
    , order = req.params.order;

  if(order == 'desc'){
	  var swap = from;
	  from = -1-to;
	  to = -1-swap;
  }
  
  Job.range(from, to, order, function(err, jobs){
    if (err) return safeSend(res, { error: err.message });
    safeSend(res, jobs);
  });
};

/**
 * Get jobs by :state, and range :from..:to.
 */

exports.jobStateRange = function(req, res){
  var state = req.params.state
    , from = parseInt(req.params.from, 10)
    , to = parseInt(req.params.to, 10)
    , order = req.params.order;

  if(order == 'desc'){
	  var swap = from;
	  from = -1-to;
	  to = -1-swap;
  }
  
  Job.rangeByState(state, from, to, order, function(err, jobs){
    if (err) return safeSend(res, { error: err.message });
    safeSend(res, jobs);
  });
};

/**
 * Get jobs by :type, :state, and range :from..:to.
 */

exports.jobTypeRange = function(req, res){
  var type = req.params.type
    , state = req.params.state
    , from = parseInt(req.params.from, 10)
    , to = parseInt(req.params.to, 10)
    , order = req.params.order;

  if(order == 'desc'){
	  var swap = from;
	  from = -1-to;
	  to = -1-swap;
  }
  
  Job.rangeByType(type, state, from, to, order, function(err, jobs){
    if (err) return safeSend(res, { error: err.message });
    safeSend(res, jobs);
  });
};

/**
 * Get job by :id.
 */

exports.job = function(req, res){
  var id = req.params.id;
  Job.get(id, function(err, job){
    if (err) return safeSend(res, { error: err.message });
    safeSend(res, job);
  });
};

/**
 * Remove job :id.
 */

exports.remove = function(req, res){
  var id = req.params.id;
  Job.remove(id, function(err){
    if (err) return safeSend(res, { error: err.message });
    safeSend(res, { message: 'job ' + id + ' removed' });
  });
};

/**
 * Update job :id :priority.
 */

exports.updatePriority = function(req, res){
  var id = req.params.id
    , priority = parseInt(req.params.priority, 10);

  if (isNaN(priority)) return safeSend(res, { error: 'invalid priority' });
  Job.get(id, function(err, job){
    if (err) return safeSend(res, { error: err.message });
    job.priority(priority);
    job.save(function(err){
      if (err) return safeSend(res, { error: err.message });
      safeSend(res, { message: 'updated priority' });
    });
  });
};

/**
 * Update job :id :state.
 */

exports.updateState = function(req, res){
  var id = req.params.id
    , state = req.params.state;

  Job.get(id, function(err, job){
    if (err) return safeSend(res, { error: err.message });
    job.state(state, function(err) {
      if (err) return safeSend(res, { error: err.message });
      safeSend(res, { message: 'updated state' });
    });
  });
};

/**
 * Search and respond with ids.
 */

exports.search = function(req, res){
  getSearch().query(req.query.q, function(err, ids){
    if (err) return safeSend(res, { error: err.message });
    safeSend(res, ids);
  });
};

/**
 * Get log for job :id.
 */

exports.log = function(req, res){
  var id = req.params.id;
  Job.log(id, function(err, log){
    if (err) return safeSend(res, { error: err.message });
    safeSend(res, log);
  });
};

/**
 * Data fetching helper.
 */

function get(obj) {
  var pending = 0
    , res = {}
    , callback
    , done;

  return function _(arg){
    switch (typeof arg) {
      case 'function':
        callback = arg;
        break;
      case 'string':
        ++pending;
        obj[arg](function(err, val){
          if (done) return;
          if (err) return done = true, callback(err);
          res[arg] = val;
          --pending || callback(null, res);
        });
        break;
    }
    return _;
  };
}

function safeSend(res, data) {
    try {
        res.send(data);
    } catch (error) {
        // ignore errors on send
    }
}