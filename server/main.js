Meteor.methods({
    restart: function(){restart(0);},										   //不能直接执行process.exit(),要异步执行,否则会反复restart
});

function restart(delay){
	Log.insert({task:'init', lastRun:new Date(), error:true, info:'Restart...'},function(){
		console.log('Restart...');
		Meteor.setTimeout(function(){process.exit(101);},delay);
	});
}

Meteor.publish('ticker', function(){return Ticker.find();});
Meteor.publish('task',function(){return Task.find();});
Meteor.publish('log',function(){return Log.find({error:true});});
Meteor.publish('trade',function(src,start,end){return Trade.find({src:src,tid:{$gte:start,$lte:end}});});

util = Npm.require('util');

Meteor.startup(function () {
    init();
    getTicker();
    Meteor.setInterval(getTicker, 15000);            //15 sec
    getBitfinex();
    Meteor.setInterval(getBitfinex, 1000*60);        //60 sec, 1 min     //max till now, 702 / 5 min (bitfinex), 1912 / 5 min (bitstamp)
    getBitstamp();
    Meteor.setInterval(getBitstamp, 1000*60*10);     //600 sec, 10 min
    clean();
    Meteor.setInterval(clean, 1000*60*60*12);        //half day
});

function init(){
    var bitfinexControl = { task:'bitfinex', lastRun:null, dateResp:null, error:null, info:'', count:0, all:0, dateLast:0, tidLast:0, cache:'',errorCount:0 };
    var bitstampControl = { task:'bitstamp', lastRun:null, dateResp:null, error:null, info:'', count:0, all:0, dateLast:0, tidLast:0, cache:'',errorCount:0 };
    var cleanControl    = { task:'clean',    lastRun:null, dateResp:null, error:null, info:'', count:0, all:0, daysKept:2 };

    if (Task.find({task:'bitfinex'}).count()===0) {
        Task.insert(bitfinexControl);
    } else {
        bitfinexControl = Task.findOne({task:'bitfinex'});
        Trade.remove({tid:{$gt:bitfinexControl.tidLast},src:'bitfinex'});
    };
    if (Task.find({task:'bitstamp'}).count()===0) {
        Task.insert(bitstampControl);
    } else {
        bitstampControl = Task.findOne({task:'bitstamp'});
        Trade.remove({tid:{$gt:bitstampControl.tidLast},src:'bitstamp'});
    };
    if (Task.find({task:'clean'}).count()===0) {
        Task.insert(cleanControl);
    };
    Log.insert({task:'init', lastRun:new Date(), error:true, info:'Done...'});
}

function getTicker(){
    HTTP.get('https://www.bitstamp.net/api/ticker/',{timeout:45000},function(err,res){
        if (!err) Ticker.upsert({src:'bitstamp'},
            {src:'bitstamp',ask:res.data.ask,bid:res.data.bid,last:res.data.last,date:new Date()});
    });
    HTTP.get('https://api.bitfinex.com/v1/pubticker/btcusd',{timeout:45000},function(err,res){
        if (!err) Ticker.upsert({src:'bitfinex'},
            {src:'bitfinex',ask:res.data.ask,bid:res.data.bid,last:res.data.last_price,date:new Date(res.data.timestamp * 1000)});
    });
}

function getBitfinex(){
    if (!lock('bitfinex')) return;
    var ctrl = Task.findOne({task:'bitfinex'});
    if (!ctrl) return;
    ctrl.lastRun = new Date(), ctrl.error = true, ctrl.count=0, ctrl.all=0;

    var options = {params:{timestamp:ctrl.dateLast},headers:{'If-None-Match':ctrl.cache},timeout:45000};
    HTTP.get('https://api.bitfinex.com/v1/trades/btcusd',options,function(err,res){
        ctrl.dateResp = new Date();
        if (err) {
            ctrl.info = Match.test(err,{response:Match.ObjectIncluding({statusCode:Match.Any})})? err.response.statusCode : err;
        } else if (res.statusCode === 304) {
            ctrl.error = false,ctrl.info = res.statusCode;
        } else if (res.statusCode !== 200) {
            ctrl.info = res.statusCode;
        } else if (!res.data) {
            ctrl.info = 'no data';
        } else {
            ctrl.error = false,ctrl.info = '',ctrl.cache = res.headers['etag'];
            saveBitfinex(res.data,ctrl);
        }

		ctrl.errorCount = (ctrl.error)? (ctrl.errorCount || 0) + 1 : 0;

        Log.insert(_.omit(ctrl,'_id'),function(err,id){
            if (err) console.log('getBitfinex insert error:', err);
        });
        Task.update(ctrl._id, ctrl,function(err,num){
            if (err) {
                console.log('getBitfinex update error:', err);
                Log.insert({task:'bitfinex taskUpdate', lastRun:new Date(), error:true, info:err});
            }
			if (ctrl.errorCount>=3) {
				restart(1000*30);	   // 0.5min
			} else {
				unlock('bitfinex');
			}
        });
    });
}

function saveBitfinex(data,ctrl){
    var tidLast,dateLast,total,count = 0;
    total = Trade.find({src:'bitfinex'}).count();
    _.each(data,function(row,index,list){
        row.date=row.timestamp;
        if (!(row.tid>ctrl.tidLast && row.date>=ctrl.dateLast)) return;
        count++;
        if (index===0){
            tidLast = row.tid, dateLast = row.date;
        } else {
            if (row.tid>=list[index-1].tid){
                ctrl.error = true,ctrl.info = 'Data not ordered:';
            }
        }
        if (index==list.length - 1) {
            if (row.tid>ctrl.tidLast + 1) {
                ctrl.error = true,ctrl.info = util.format( 'Gap from %d to %d, <= %d, {%d, %d}',
                    ctrl.tidLast,row.tid,row.tid - ctrl.tidLast - 1, ctrl.dateLast,row.date);
            }
        }
        row.price_num=Number(row.price);
        row.amount_num=Number(row.amount);
        row.src = 'bitfinex';
        Trade.insert(_.omit(row,'timestamp'),function(err,id){
            if (err) {
                console.log('saveBitfinex insert error:', err);
                Log.insert({task:'bitfinex insert', lastRun:new Date(), error:true, info:err});
            }
        });
    });
    ctrl.total = total, ctrl.count = count, ctrl.all = data.length;
    if (count>0) {
        ctrl.tidLast = tidLast, ctrl.dateLast = dateLast;
    }
}

function getBitstamp(){
    if (!lock('bitstamp')) return;
    var ctrl = Task.findOne({task:'bitstamp'});
    if (!ctrl) return;
    ctrl.lastRun = new Date(), ctrl.error = true, ctrl.count=0, ctrl.all=0;

	var options = {params:{time:'hour'},headers:{'If-Modified-Since':ctrl.cache},timeout:90000};
    HTTP.get('https://www.bitstamp.net/api/transactions/',options,function(err,res){
        ctrl.dateResp = new Date();
        if (err) {
            ctrl.info = Match.test(err,{response:Match.ObjectIncluding({statusCode:Match.Any})})? err.response.statusCode : err;
        } else if (res.statusCode === 304) {
            ctrl.error = false,ctrl.info = res.statusCode;
        } else if (res.statusCode !== 200) {
            ctrl.info = res.statusCode;
        } else if (!res.data) {
            ctrl.info = 'no data';
        } else {
            ctrl.error = false,ctrl.info = '',ctrl.cache = res.headers['last-modified'];
            saveBitstamp(res.data,ctrl)
        }

		ctrl.errorCount = (ctrl.error)? (ctrl.errorCount || 0) + 1 : 0;

		Log.insert(_.omit(ctrl,'_id'),function(err,id){
            if (err) console.log('getBitstamp insert error:', err);
        });
        Task.update(ctrl._id, ctrl,function(err,num){
            if (err) {
                console.log('getBitstamp update error:', err);
                Log.insert({task:'bitstamp taskUpdate', lastRun:new Date(), error:true, info:err});
            }
			if (ctrl.errorCount>=3) {
				restart(1000*60*5);  // 5min
			} else {
				unlock('bitstamp');
			}
        });
    });
}

function saveBitstamp(data,ctrl){
    var tidLast,dateLast,total,count = 0;
    total = Trade.find({src:'bitstamp'}).count();
    _.each(data,function(row,index,list){
        row.date=Number(row.date);
        if (row.tid<=ctrl.tidLast) return;
        count++;
        if (index===0){
            tidLast = row.tid, dateLast = row.date;
        } else {
            if (row.tid>=list[index-1].tid){
                ctrl.error = true,ctrl.info = 'Data not ordered:';
            }
        }
        if (index==list.length - 1) {
            if (row.tid>ctrl.tidLast + 1) {
                ctrl.error = true,ctrl.info = util.format( 'Gap from %d to %d, <= %d, {%d, %d}',
                    ctrl.tidLast,row.tid,row.tid - ctrl.tidLast - 1, ctrl.dateLast,row.date);
            }
        }
        row.price_num=Number(row.price);
        row.amount_num=Number(row.amount);
        row.src = 'bitstamp';
        Trade.insert(row,function(err,id){
            if (err) {
                console.log('saveBitstamp insert error:', err);
                Log.insert({task:'bitstamp insert', lastRun:new Date(), error:true, info:err});
            }
        });
    });
    ctrl.total = total, ctrl.count = count, ctrl.all = data.length;
    if (count>0) {
        ctrl.tidLast = tidLast, ctrl.dateLast = dateLast;
    }
}

function clean(){
    var ctrl = Task.findOne({task:'clean'});
    if (!ctrl) return;
    var dateReq = new Date();
    var all = Trade.find().count();
    Trade.remove({date:{$lt:(dateReq/1000 - 60*60*24*ctrl.daysKept)}},function(err,num){
        var dateResp = new Date();
        ctrl.lastRun = dateReq, ctrl.dateResp = dateResp, ctrl.all = all;
        if (!err) {
            ctrl.error = false,ctrl.info ='',ctrl.count   = num;
        } else {
            ctrl.error = true,ctrl.info =err,ctrl.count   = 0;
        }
        Log.insert(_.omit(ctrl,'_id'),function(err,id){
            if (err) console.log('clean insert error:', err);
        });
        Task.update(ctrl._id, ctrl,function(err,num){
            if (err) {
                console.log('clean update error:', err);
                Log.insert({task:'clean taskUpdate', lastRun:new Date(), error:true, info:err});
            }
        });
    });
    Log.remove({lastRun:{$lt:new Date(dateReq - 1000*60*60*24*ctrl.daysKept)}},function(err,num){
        if (err) {
            Log.insert({task:'clean log', lastRun:new Date(), error:true, info:err});
        }
    });
}
