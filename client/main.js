Meteor.subscribe('ticker');
Meteor.subscribe('task');
Meteor.subscribe('log');

UI.registerHelper('_', function() {
  arguments = _.toArray(arguments);
  var self = this,
      fn = arguments[0];
  arguments.shift(); // Removes the Underscore function
  arguments.pop();   // Remove the Spacebars appended argument
  return _[fn].apply(self, arguments);
});

/* Example:
{{#if _ "isString" "bonjour"}}
  It is!
{{else}}
  It is not :(
{{/if}}

Would return "It is!"
*/

UI.registerHelper('$YN', function(info) {
  return info? 'Yes':'No';
});
UI.registerHelper('$json', function(info) {
  return (info instanceof Object)? EJSON.stringify(info) : info;
});
UI.registerHelper('$date', function(datum) {
  function pad2(x) {if (x<10) { return '0'+x;} else {return ''+x;}}
  return datum.getFullYear()+'/'+pad2(datum.getMonth()+1)+'/'+pad2(datum.getDate())+' '+
           pad2(datum.getHours())+':'+pad2(datum.getMinutes())+':'+pad2(datum.getSeconds());
});

Template.marketInfo.helpers({
    ticker  : function() { return Ticker.find();},
    task    : function() { return Task.find();},
    log     : function() { return Log.find({},{sort:[['lastRun','desc']]});},
});

Src  = new Meteor.Collection(null);
Src.insert({_id:'1',text:'bitstamp'});
Src.insert({_id:'2',text:'bitfinex'});

Session.setDefault('searchSrc', 'bitfinex')		 //selected Src
Session.setDefault('searchFrom', 0);
Session.setDefault('searchTo', 0);
Session.setDefault('searchInfo', '');
Session.setDefault('searchResult', '');

Template.marketInfo.events({
    'click input.restart'   : function () {	Meteor.call('restart');	},
});
Template.search.helpers({
    srcList  : function() { return Src.find();},
    info     : function() { return Session.get('searchInfo');},
    result   : function() { return Session.get('searchResult');},
});
Template.srcInfo.helpers({
	selected: function() { return Session.equals("searchSrc", this.text) ? "selected" : "";},
});
Template.search.events({
    'change select.src'       : function (event) {
		Session.set('searchSrc', event.target.value);
	},
    'blur input.from'       : function (event) {
		Session.set('searchFrom', +event.target.value);
	},
    'blur input.to'       : function (event) {
		Session.set('searchTo', +event.target.value);
	},
    'click input.search'   : function () {
		Session.set('searchInfo', 'Loading...');
		Session.set('searchResult', '');
		var src=Session.get('searchSrc'),from=Session.get('searchFrom'),to=Session.get('searchTo');
		Meteor.subscribe('trade',src,from,to,{
            onReady:function(){
				var cursor = Trade.find({src:src,tid:{$gte:from,$lte:to}},{sort:['tid'],fields:{src:0, _id:0}})
				Session.set('searchInfo', 'Done(' + cursor.count() + ')...');
				Session.set('searchResult', EJSON.stringify(cursor.fetch()));
			},
            onError:function(err){
				Session.set('searchInfo', 'Error...');
				Session.set('searchResult', EJSON.stringify(err));
			}
		});
	},
});
