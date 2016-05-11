'use strict';

var Readable = require('stream').Readable;
var util = require('util');

function MatchStream(opt) {

  Readable.call(this, opt);

  this.opt = opt;
  this.results = false;

  this.scan();

}

util.inherits(MatchStream, Readable);

MatchStream.prototype.scan = function() {

  var lua = "local cursor = 0 \
    local response = {} \
    local matches \
    local scan \
    repeat \
      scan = redis.call('SCAN', cursor, 'MATCH', ARGV[1])\
      cursor = tonumber(scan[1]) \
      matches = scan[2] \
      for i, key in ipairs(matches) do \
        response[i] = key \
      end \
    until cursor == 0 \
    return cjson.encode(response)";

  var _this = this;

  this.opt.redis.eval(lua, 0, this.opt.match, function(err, res) {

    if(err) {
      _this.emit('error', err);
      return;
    }

    try {

      if(res != '{}') {
        _this.results = JSON.parse(res);
      } else {
        _this.results = null;
      }

      _this.emit('ready');

    } catch(e) {
      _this.emit('error', e);
    }

  });

};

MatchStream.prototype._read = function() {

  if(this.results === false) {
    return this.once('ready', this._read);
  }

  var results = this.results;
  this.results = null;

  this.push(results);

};

module.exports = MatchStream;
