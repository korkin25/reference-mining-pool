var fs = require('fs');
var net = require('net');
var crypto = require('crypto');

var async = require('async');
var bignum = require('bignum');
var cnUtil = require('cryptoforknote-util');

// Must exactly be 8 hex chars, already lowercased before test
var noncePattern = new RegExp("^[0-9a-f]{8}$");

//SSL for claymore
var tls = require('tls');

var threadId = '(Thread ' + process.env.forkId + ') ';

var logSystem = 'pool';
require('./exceptionWriter.js')(logSystem);

var apiInterfaces = require('./apiInterfaces.js')(config.daemon, config.wallet, config.api);
var utils = require('./utils.js');
Buffer.prototype.toByteArray = function () {
  return Array.prototype.slice.call(this, 0)
}

var log = function(severity, system, text, data){
    global.log(severity, system, threadId + text, data);
};

var multiHashing = require('cryptonight-hashing');


var diff1 = bignum('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF', 16);

var instanceId = crypto.randomBytes(4);

var validBlockTemplates = [];
var currentBlockTemplate;
var currentBlockHeight = 0;
var currentBlockHash = "";

var connectedMiners = {};

var bannedIPs = {};
var perIPStats = {};

var shareTrustEnabled = config.poolServer.shareTrust && config.poolServer.shareTrust.enabled;
var shareTrustStepFloat = shareTrustEnabled ? config.poolServer.shareTrust.stepDown / 100 : 0;
var shareTrustMinFloat = shareTrustEnabled ? config.poolServer.shareTrust.min / 100 : 0;


var banningEnabled = config.poolServer.banning && config.poolServer.banning.enabled;

var walletBase58AddressPrefix = config.poolServer.walletBase58AddressPrefix;
var walletBase58IntAddressPrefix = config.poolServer.walletBase58IntAddressPrefix;
var walletBase58SubAddressPrefix = config.poolServer.walletBase58SubAddressPrefix;

/* Every 30 seconds clear out timed-out miners and old bans */
setInterval(function(){
    var now = Date.now();
    var timeout = config.poolServer.minerTimeout * 1000;
    for (var minerId in connectedMiners){
        var miner = connectedMiners[minerId];
        if (now - miner.lastBeat > timeout){
            //log('warn', logSystem, 'Miner timed out and disconnected %s@%s', [miner.login, miner.ip]);
            delete connectedMiners[minerId];
        }
    }

    if (banningEnabled){
        for (ip in bannedIPs){
            var banTime = bannedIPs[ip];
            if (now - banTime > config.poolServer.banning.time * 1000) {
                delete bannedIPs[ip];
                delete perIPStats[ip];
                log('info', logSystem, 'Ban dropped for %s', [ip]);
            }
        }
    }

}, 30000);


process.on('message', function(message) {
    switch (message.type) {
        case 'banIP':
            bannedIPs[message.ip] = Date.now();
            break;
    }
});


function IsBannedIp(ip){
    if (!banningEnabled || !bannedIPs[ip]) return false;

    var bannedTime = bannedIPs[ip];
    var bannedTimeAgo = Date.now() - bannedTime;
    var timeLeft = config.poolServer.banning.time * 1000 - bannedTimeAgo;
    if (timeLeft > 0){
        return true;
    }
    else {
        delete bannedIPs[ip];
        log('info', logSystem, 'Ban dropped for %s', [ip]);
        return false;
    }
}


function BlockTemplate(template){
    this.blob = template.blocktemplate_blob;
    this.difficulty = template.difficulty;
    this.height = template.height;
    this.reserveOffset = template.reserved_offset;
    this.buffer = Buffer.from(this.blob, 'hex');
    instanceId.copy(this.buffer, this.reserveOffset + 4, 0, 3);
    this.previous_hash = Buffer.alloc(32);
    this.buffer.copy(this.previous_hash,0,7,39);
    this.extraNonce = 0;
    this.seed_hash = template.seed_hash;
}
BlockTemplate.prototype = {
    nextBlob: function(){
        this.buffer.writeUInt32BE(++this.extraNonce, this.reserveOffset);
        return cnUtil.convert_blob(this.buffer).toString('hex');
    }
};



function getBlockTemplate(callback){
    apiInterfaces.rpcDaemon('getblocktemplate', {reserve_size: 8, wallet_address: config.poolServer.poolAddress}, callback);
}

function getBlockCount(callback){
    apiInterfaces.rpcDaemon('getblockcount', null, callback);
}

function getBlockHash(callback){
    apiInterfaces.rpcDaemon('on_getblockhash', [currentBlockHeight - 1], callback);
}

function jobLoop()
{
    jobRefresh();
    setTimeout(function(){ jobLoop(); }, config.poolServer.blockRefreshInterval);
}

var jobRefreshCompleteCallback = null;
function jobRefreshError(text, error)
{
    log('error', logSystem, text, [error]);
    if(jobRefreshCompleteCallback != null)
        jobRefreshCompleteCallback(false);
}

var jobRefreshCounter = 0;
function jobRefresh(state){
    state = state || "check_force";

    switch(state){
    case "check_force":
        if(jobRefreshCounter % config.poolServer.blockRefreshForce == 0)
            jobRefresh("get_template");
        else
            jobRefresh("check_count");
        jobRefreshCounter++;
        break;

    case "check_count":
        getBlockCount(function(error, result){
            if (error){
                jobRefreshError('Error polling getblockcount %j', error);
                return;
            }

            if(result.count == currentBlockHeight) {
                jobRefresh("check_hash");
                return;
            }

            log('info', logSystem, 'Blockchain height changed to %d, updating template.', [currentBlockHeight]);
            jobRefresh("get_template");
            return;
        });
	break;

    case "check_hash":
	getBlockHash(function(error, result){
	    if(error) {
			jobRefreshError('Error polling on_getblockhash %j', error);
                return;
            }

            if(result == currentBlockHash) {
                if(jobRefreshCompleteCallback != null)
                     jobRefreshCompleteCallback(true);
                return;
            }

            log('info', logSystem, 'Blockchain hash changed to %s, updating template.', [currentBlockHash]);
            jobRefresh("get_template");
            return;
        });
        break;

    case "get_template":
        getBlockTemplate(function(error, result){
            if(error) {
		        jobRefreshError('Error polling getblocktemplate %j', error);
                return;
            }

            currentBlockHeight = result.height;
            currentBlockHash = result.prev_hash;

            var buffer = Buffer.from(result.blocktemplate_blob, 'hex');
            var previous_hash = Buffer.alloc(32);
            buffer.copy(previous_hash,0,7,39);
            if (!currentBlockTemplate || previous_hash.toString('hex') != currentBlockTemplate.previous_hash.toString('hex')){
                log('info', logSystem, 'New block to mine at height %d w/ difficulty of %d', [result.height, result.difficulty]);
                processBlockTemplate(result);
            }

            if(jobRefreshCompleteCallback != null)
                jobRefreshCompleteCallback(true);
        });
    }
}



function processBlockTemplate(template){

    if (currentBlockTemplate)
        validBlockTemplates.push(currentBlockTemplate);

    if (validBlockTemplates.length > 3)
        validBlockTemplates.shift();

    currentBlockTemplate = new BlockTemplate(template);

    for (var minerId in connectedMiners){
        var miner = connectedMiners[minerId];
        if(!miner.noRetarget) {
            var now = Date.now() / 1000 | 0;
            miner.retarget(now);
        }
        miner.pushMessage('job', miner.getJob());
    }
}



(function init(){
    jobRefreshCompleteCallback = function(sucessful){
        if (!sucessful){
            log('error', logSystem, 'Could not start pool');
            return;
        }
        startPoolServerTcp(function(successful){ });
        jobRefreshCompleteCallback = null;
    };

    jobLoop();
})();

var VarDiff = (function(){
    var variance = config.poolServer.varDiff.variancePercent / 100 * config.poolServer.varDiff.targetTime;
    return {
        variance: variance,
        bufferSize: config.poolServer.varDiff.retargetTime / config.poolServer.varDiff.targetTime * 4,
        tMin: config.poolServer.varDiff.targetTime - variance,
        tMax: config.poolServer.varDiff.targetTime + variance,
        maxJump: config.poolServer.varDiff.maxJump
    };
})();

function Miner(id, login, pass, ip, startingDiff, noRetarget, pushMessage){
    this.id = id;
    this.login = login;
    this.pass = pass;
    this.ip = ip;
    this.pushMessage = pushMessage;
    this.heartbeat();
    this.noRetarget = noRetarget;
    this.difficulty = startingDiff;
    this.validJobs = [];

    // Vardiff related variables
    this.shareTimeRing = utils.ringBuffer(16);
    this.lastShareTime = Date.now() / 1000 | 0;

    this.validShares = 0;
    this.invalidShares = 0;

    if (shareTrustEnabled) {
        this.trust = {
            threshold: config.poolServer.shareTrust.threshold,
            probability: 1,
            penalty: 0
        };
    }
}
Miner.prototype = {
    retarget: function(now){

        var options = config.poolServer.varDiff;

        var sinceLast = now - this.lastShareTime;
        var decreaser = sinceLast > VarDiff.tMax;

        var avg = this.shareTimeRing.avg(decreaser ? sinceLast : null);
        var newDiff;

        var direction;

        if (avg > VarDiff.tMax && this.difficulty > options.minDiff){
            newDiff = options.targetTime / avg * this.difficulty;
            newDiff = newDiff > options.minDiff ? newDiff : options.minDiff;
            direction = -1;
        }
        else if (avg < VarDiff.tMin && this.difficulty < options.maxDiff){
            newDiff = options.targetTime / avg * this.difficulty;
            newDiff = newDiff < options.maxDiff ? newDiff : options.maxDiff;
            direction = 1;
        }
        else{
            return;
        }

        if (Math.abs(newDiff - this.difficulty) / this.difficulty * 100 > options.maxJump){
            var change = options.maxJump / 100 * this.difficulty * direction;
            newDiff = this.difficulty + change;
        }

        newDiff = Math.round(newDiff);
        this.pendingDifficulty = newDiff;
        this.shareTimeRing.clear();
        if (decreaser) this.lastShareTime = now;
    },
    heartbeat: function(){
        this.lastBeat = Date.now();
    },
    getTargetHex: function(){
        if (this.pendingDifficulty){
            this.lastDifficulty = this.difficulty;
            this.difficulty = this.pendingDifficulty;
            this.pendingDifficulty = null;
        }

        var padded = Buffer.alloc(32);
        padded.fill(0);

        var diffBuff = diff1.div(this.difficulty).toBuffer();
        diffBuff.copy(padded, 32 - diffBuff.length);

        var buff = padded.slice(0, 4);
        var buffArray = buff.toByteArray().reverse();
        var buffReversed = Buffer.from(buffArray);
        this.target = buffReversed.readUInt32BE(0);
        var hex = buffReversed.toString('hex');
        return hex;
    },
    getJob: function(){
        if (this.lastBlockHeight === currentBlockTemplate.height && !this.pendingDifficulty) {
            return {
                blob: '',
                job_id: '',
                target: ''
            };
        }

        var blob = currentBlockTemplate.nextBlob();
        this.lastBlockHeight = currentBlockTemplate.height;
        var target = this.getTargetHex();

        var newJob = {
            id: utils.uid(),
            extraNonce: currentBlockTemplate.extraNonce,
            height: currentBlockTemplate.height,
            difficulty: this.difficulty,
            diffHex: this.diffHex,
            submissions: []
        };

        this.validJobs.push(newJob);

        if (this.validJobs.length > 4)
            this.validJobs.shift();

        return {
            blob: blob,
            seed_hash: currentBlockTemplate.seed_hash,
            job_id: newJob.id,
			algo: "rx/v",
            target: target
        };
    },
    checkBan: function(validShare){
        if (!banningEnabled) return;
        // Store valid/invalid shares per IP (already initialized with 0s)
        // Init global per-IP shares stats
        if (!perIPStats[this.ip]){
            perIPStats[this.ip] = { validShares: 0, invalidShares: 0 };
        }
        var stats = perIPStats[this.ip];
        validShare ? stats.validShares++ : stats.invalidShares++;

        if (stats.validShares + stats.invalidShares >= config.poolServer.banning.checkThreshold){
            if (stats.invalidShares / stats.validShares >= config.poolServer.banning.invalidPercent / 100){
                log('warn', logSystem, 'Banned %s@%s', [this.login, this.ip]);
                bannedIPs[this.ip] = Date.now();
                delete connectedMiners[this.id];
                process.send({type: 'banIP', ip: this.ip});
            }
            else{
                stats.invalidShares = 0;
                stats.validShares = 0;
            }
        }
    }
};



function recordShareData(miner, job, shareDiff, blockCandidate, hashHex, shareType, blockTemplate){

    var dateNow = Date.now();
    var dateNowSeconds = dateNow / 1000 | 0;

    var redisCommands = [
        ['hincrby', config.coin + ':shares:roundCurrent', miner.login, job.difficulty],
        ['zadd', config.coin + ':workerHashrate', dateNowSeconds, [job.difficulty, miner.login+':'+miner.pass, dateNow,0].join(':')],
        ['zadd', config.coin + ':hashrate', dateNowSeconds, [job.difficulty, miner.login, dateNow].join(':')],
        ['hincrby', config.coin + ':workers:' + miner.login, 'hashes', job.difficulty],
        ['hset', config.coin + ':workers:' + miner.login, 'lastShare', dateNowSeconds]
    ];

    if (blockCandidate){
        redisCommands.push(['hset', config.coin + ':stats', 'lastBlockFound', Date.now()]);
        redisCommands.push(['rename', config.coin + ':shares:roundCurrent', config.coin + ':shares:round' + job.height]);
        redisCommands.push(['hgetall', config.coin + ':shares:round' + job.height]);
    }

    redisClient.multi(redisCommands).exec(function(err, replies){
        if (err){
            log('error', logSystem, 'Failed to insert share data into redis %j \n %j', [err, redisCommands]);
            return;
        }
        if (blockCandidate){
            var workerShares = replies[replies.length - 1];
            var totalShares = Object.keys(workerShares).reduce(function(p, c){
                return p + parseInt(workerShares[c]);
            }, 0);
            redisClient.zadd(config.coin + ':blocks:candidates', job.height, [
                hashHex,
                Date.now() / 1000 | 0,
                blockTemplate.difficulty,
                totalShares,
				null,null,0
            ].join(':'), function(err, result){
                if (err){
                    log('error', logSystem, 'Failed inserting block candidate %s \n %j', [hashHex, err]);
                }
            });
        }

    });

    log('info', logSystem, 'Accepted %s share at difficulty %d/%d from %s@%s', [shareType, job.difficulty, shareDiff, miner.login, miner.ip]);

}

function processShare(miner, job, blockTemplate, nonce, resultHash,sendReply){
    var template = Buffer.alloc(blockTemplate.buffer.length);
    blockTemplate.buffer.copy(template);
    template.writeUInt32BE(job.extraNonce, blockTemplate.reserveOffset);
    var shareBuffer = cnUtil.construct_block_blob(template, Buffer.from(nonce, 'hex'));
    //var shareBuffer = cnUtil.construct_block_blob(template, Buffer.from('00000000', 'hex'));

    var convertedBlob;
    var shareType;

	var resultHashBuffer = Buffer.from(resultHash, 'hex');
	var resultHashArray = resultHashBuffer.toByteArray().reverse();
    var resultHashNum = bignum.fromBuffer(Buffer.from(resultHashArray));
    var resultHashDiff = diff1.div(resultHashNum);

    if (resultHashDiff.ge(blockTemplate.difficulty))
    {
		shareType = 'trusted';

        apiInterfaces.rpcDaemon('submitblock', [shareBuffer.toString('hex')], function(error, result){
            convertedBlob = cnUtil.convert_blob(shareBuffer);
            var hash = multiHashing.randomx(convertedBlob, Buffer.from(blockTemplate.seed_hash, 'hex'), 19);

			var hashArray = hash.toByteArray().reverse();
			var hashNum = bignum.fromBuffer(Buffer.from(hashArray));
			var hashDiff = diff1.div(hashNum);

			if (error){
				if (hashDiff.ge(blockTemplate.difficulty)){
					if (hash.toString('hex') === resultHash) {
						shareType = 'valid';
						log('error', logSystem, 'Error submitting block at height %d from %s@%s, share type: "%s" - %j', [job.height, miner.login, miner.ip, shareType, error]);
						recordShareData(miner, job, hashDiff.toString(), false, null, shareType);
					}
				}
			}
			else{
				shareType = 'valid';
				var blockFastHash = cnUtil.get_block_id(shareBuffer).toString('hex');
				log('info', logSystem,
					'Block %s found at height %d by miner %s@%s - submit',
					[blockFastHash.substr(0, 6), job.height, miner.login, miner.ip]
				);
				recordShareData(miner, job, hashDiff.toString(), true, blockFastHash, shareType, blockTemplate);
				jobRefresh("get_template");
			}
        });
    }


    convertedBlob = cnUtil.convert_blob(shareBuffer);
	var hash = multiHashing.randomx(convertedBlob, Buffer.from(blockTemplate.seed_hash, 'hex'), 19);

	shareType = 'valid';

	var dateNow = Date.now();
	var dateNowSeconds = dateNow / 1000 | 0;

	if (hash.toString('hex') !== resultHash) {
		redisClient.zadd(config.coin + ':workerHashrate', dateNowSeconds, [job.difficulty, miner.login+':'+miner.pass, dateNow,1].join(':'));
		log('warn', logSystem, 'Bad hash from miner %s@%s', [miner.login, miner.ip]);
		miner.checkBan(false);
		sendReply('Bad share');
	}
	else
	{
		var hashArray = hash.toByteArray().reverse();
		var hashNum = bignum.fromBuffer(Buffer.from(hashArray));
		var hashDiff = diff1.div(hashNum);

		if (hashDiff.ge(blockTemplate.difficulty)){
			var now = Date.now() / 1000 | 0;
			miner.shareTimeRing.append(now - miner.lastShareTime);
			miner.lastShareTime = now;
			miner.checkBan(true);
			sendReply(null, {status: 'OK'});
		}
		else if (hashDiff.lt(job.difficulty)){
			log('warn', logSystem, 'Rejected low difficulty share of %s from %s@%s', [hashDiff.toString(), miner.login, miner.ip]);
			redisClient.zadd(config.coin + ':workerHashrate', dateNowSeconds, [job.difficulty, miner.login+':'+miner.pass, dateNow,2].join(':'));
			miner.checkBan(false);
			sendReply('Bad share');
		}
		else{
			recordShareData(miner, job, hashDiff.toString(), false, null, shareType);
			var now = Date.now() / 1000 | 0;
			miner.shareTimeRing.append(now - miner.lastShareTime);
			miner.lastShareTime = now;
			miner.checkBan(true);
			sendReply(null, {status: 'OK'});
		}
	}
}


function handleMinerMethod(method, params, ip, portData, sendReply, pushMessage){


    var miner = connectedMiners[params.id];
    // Check for ban here, so preconnected attackers can't continue to screw you
    if (IsBannedIp(ip)){
        sendReply('your IP is banned');
        return;
    }
    switch(method){
        case 'login':
            var login = params.login;
            if (!login){
                sendReply('missing login');
                return;
            }

            var difficulty = portData.difficulty;
            var noRetarget = false;
            if(config.poolServer.fixedDiff.enabled) {
                var fixedDiffCharPos = login.indexOf(config.poolServer.fixedDiff.addressSeparator);
                if(fixedDiffCharPos != -1) {
                    noRetarget = true;
                    difficulty = login.substr(fixedDiffCharPos + 1);
                    if(difficulty < config.poolServer.varDiff.minDiff) {
                        difficulty = config.poolServer.varDiff.minDiff;
                    }
                    login = login.substr(0, fixedDiffCharPos);
                    log('info', logSystem, 'Miner difficulty fixed to %s',  [difficulty]);
                }
            }

            var addressPrefix  = cnUtil.address_decode(Buffer.from(login));
            var addressPrefixi = cnUtil.address_decode_integrated(Buffer.from(login));
            if ((addressPrefix != walletBase58SubAddressPrefix)&&(addressPrefix != walletBase58AddressPrefix) &&( addressPrefixi != walletBase58IntAddressPrefix)) {
                log('info', logSystem, 'invalid addr ('+addressPrefix+')'+login);
                sendReply('invalid address used');
                return;
            }

            var minerId = utils.uid();
            if(params.pass)
			{
				var b = Buffer.from(params.pass);
                miner = new Miner(minerId, login, b.toString('base64'), ip, difficulty, noRetarget, pushMessage);
			}
			else
                miner = new Miner(minerId, login, '', ip, difficulty, noRetarget, pushMessage);

            connectedMiners[minerId] = miner;
            redisClient.sadd(config.coin + ':workers', miner.login);

            sendReply(null, {
                id: minerId,
                job: miner.getJob(),
                status: 'OK'
            });
            log('info', logSystem, 'Miner connected %s@%s',  [params.login, miner.ip]);
            break;
        case 'getjob':
            if (!miner){
                sendReply('Unauthenticated');
                return;
            }
            miner.heartbeat();
            sendReply(null, miner.getJob());
            break;
        case 'submit':
            if (!miner){
                sendReply('Unauthenticated');
                return;
            }
            miner.heartbeat();

            var job = miner.validJobs.filter(function(job){
                return job.id === params.job_id;
            })[0];

            if (!job){
                sendReply('Invalid job id');
                return;
            }

			var dateNow = Date.now();
            var dateNowSeconds = dateNow / 1000 | 0;

	        params.nonce = params.nonce.substr(0, 8).toLowerCase();

            if (!noncePattern.test(params.nonce)) {
                 var minerText = miner ? (' ' + miner.login + '@' + miner.ip) : '';
                log('warn', logSystem, 'Malformed nonce: ' + JSON.stringify(params) + ' from ' + minerText);
                 perIPStats[miner.ip] = { validShares: 0, invalidShares: 999999 };
                 miner.checkBan(false);
				redisClient.zadd(config.coin + ':workerHashrate', dateNowSeconds, [job.difficulty, miner.login+':'+miner.pass, dateNow,6].join(':'));
                 sendReply('Duplicate share');
                 return;
            }

            if (job.submissions.indexOf(params.nonce) !== -1){
                var minerText = miner ? (' ' + miner.login + '@' + miner.ip) : '';
                log('warn', logSystem, 'Duplicate share: ' + JSON.stringify(params) + ' from ' + minerText);
				redisClient.zadd(config.coin + ':workerHashrate', dateNowSeconds, [job.difficulty, miner.login+':'+miner.pass, dateNow,3].join(':'));
                perIPStats[miner.ip] = { validShares: 0, invalidShares: 999999 };
                miner.checkBan(false);
                sendReply('Duplicate share');
                return;
            }

            job.submissions.push(params.nonce);

            var blockTemplate = currentBlockTemplate.height === job.height ? currentBlockTemplate : validBlockTemplates.filter(function(t){
                return t.height === job.height;
            })[0];

            if (!blockTemplate){
                var minerText = miner ? (' ' + miner.login + '@' + miner.ip) : '';
                log('warn', logSystem, 'Block expired, Height: ' + job.height + ' from ' + minerText);
				redisClient.zadd(config.coin + ':workerHashrate', dateNowSeconds, [job.difficulty, miner.login+':'+miner.pass, dateNow,4].join(':'));
                sendReply('Block expired');
                return;
            }

			if (isNaN(job.difficulty)){
                sendReply('Invalid difficulty share');
				log('info', logSystem, 'invalid diff %d', [job.difficulty]);
                return;
            }

            var shareAccepted = processShare(miner, job, blockTemplate, params.nonce, params.result,sendReply);
            break;
        case 'keepalived' :
            if(miner) miner.heartbeat();
            sendReply(null, { status:'KEEPALIVED' });
            break;
        default:
            sendReply("invalid method");
            var minerText = miner ? (' ' + miner.login + '@' + miner.ip) : '';
            log('warn', logSystem, 'Invalid method: %s (%j) from %s', [method, params, minerText]);
            break;
    }
}


var httpResponse = ' 200 OK\nContent-Type: text/plain\nContent-Length: 20\n\nmining server online';


function startPoolServerTcp(callback){
    async.each(config.poolServer.ports, function(portData, cback){
        var handleMessage = function(socket, jsonData, pushMessage){
            if (!jsonData.id) {
                log('warn', logSystem, 'Miner RPC request missing RPC id');
                return;
            }
            else if (!jsonData.method) {
                log('warn', logSystem, 'Miner RPC request missing RPC method');
                return;
            }
            else if (!jsonData.params) {
                log('warn', logSystem, 'Miner RPC request missing RPC params');
                return;
            }

            var sendReply = function(error, result){
                if(!socket.writable) return;
                var sendData = JSON.stringify({
                    id: jsonData.id,
                    jsonrpc: "2.0",
                    error: error ? {code: -1, message: error} : null,
                    result: result
                }) + "\n";
                socket.write(sendData);
            };

            handleMinerMethod(jsonData.method, jsonData.params, socket.remoteAddress, portData, sendReply, pushMessage);
        };

        var socketResponder = function(socket){
            socket.setKeepAlive(true);
            socket.setEncoding('utf8');

            var dataBuffer = '';

            var pushMessage = function(method, params){
                if(!socket.writable) return;
                var sendData = JSON.stringify({
                    jsonrpc: "2.0",
                    method: method,
                    params: params
                }) + "\n";
                socket.write(sendData);
            };

            socket.on('data', function(d){
                dataBuffer += d;
                if (Buffer.byteLength(dataBuffer, 'utf8') > 10240){ //10KB
                    dataBuffer = null;
                    log('warn', logSystem, 'Socket flooding detected and prevented from %s', [socket.remoteAddress]);
                    socket.destroy();
                    return;
                }
                if (dataBuffer.indexOf('\n') !== -1){
                    var messages = dataBuffer.split('\n');
                    var incomplete = dataBuffer.slice(-1) === '\n' ? '' : messages.pop();
                    for (var i = 0; i < messages.length; i++){
                        var message = messages[i];
                        if (message.trim() === '') continue;
                        var jsonData;
                        try{
                            jsonData = JSON.parse(message);
                        }
                        catch(e){
                            if (message.indexOf('GET /') === 0) {
                                if (message.indexOf('HTTP/1.1') !== -1) {
                                    socket.end('HTTP/1.1' + httpResponse);
                                    break;
                                }
                                else if (message.indexOf('HTTP/1.0') !== -1) {
                                    socket.end('HTTP/1.0' + httpResponse);
                                    break;
                                }
                            }

                            log('warn', logSystem, 'Malformed message from %s: %s', [socket.remoteAddress, message]);
                            socket.destroy();

                            break;
                        }
                        handleMessage(socket, jsonData, pushMessage);
                    }
                    dataBuffer = incomplete;
                }
            }).on('error', function(err){
                if (err.code !== 'ECONNRESET')
                    log('warn', logSystem, 'Socket error from %s %j', [socket.remoteAddress, err]);
            }).on('close', function(){
                pushMessage = function(){};
            });

        };

        if(portData.type === 'SSL') {
          var options = {
            key: fs.readFileSync(config.poolServer.sslKey),
            cert: fs.readFileSync(config.poolServer.sslCert)
          };
          tls.createServer(options, socketResponder).listen(portData.port, function (error, result) {
            if (error) {
              log('error', logSystem, 'SSL Could not start server listening on port %d, error: $j', [portData.port, error]);
              cback(true);
              return;
            }
            log('info', logSystem, 'SSL Started server listening on port %d', [portData.port]);
            cback();
          });
        }
        else {
          net.createServer(socketResponder).listen(portData.port, function (error, result) {
            if (error) {
              log('error', logSystem, 'Could not start server listening on port %d, error: $j', [portData.port, error]);
              cback(true);
              return;
            }
          log('info', logSystem, 'Started server listening on port %d', [portData.port]);
          cback();
        });
      }



    }, function(err){
        if (err)
            callback(false);
        else
            callback(true);
    });
}