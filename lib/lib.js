changeAndRun = (function(k1,k2,v,fRun){
    check( fRun, Match.Optional( Function ) );

    if ( !Match.test(this[k1],Object) ) this[k1] = {};
    if ( this[k1][k2] === v ) return;

    if (!(v instanceof Object)) {
        this[k1][k2] = v;
    } else if ( '$set' in v ) {      // v.hasOwnProperty('$set') && Match.test( v, {$set:Match.Any} )
        this[k1][k2] = v.$set;
    } else if ( '$get' in v ) {      // v.hasOwnProperty('$get') && Match.test( v, {$get:String} )
        var cases ={
            it:function(x){return x;},
            keys:function(x){return x? _.keys(x) : [];},
            values:function(x){return x? _.values(x) : [];},
        };
        return cases[v.$get](this[k1][k2]);
    } else if ( '$TS' in v ) {       // v.hasOwnProperty('$TS') && Match.test( v, {$TS:Match.Any} )
        if (this[k1][k2] !== v.$TS ) {    //Test whether different and Set
            this[k1][k2] = v.$TS;
            return true;
        } else {
            console.log('$TS, false');
            return false;
        }
    } else if ( v.hasOwnProperty('$inc') && Match.test( v, {$inc:Number} ) ) {
        if (!Match.test(this[k1][k2],Number)) {
            if (this[k1][k2]) {console.log('Warning, overwritten:',this[k1][k2]);}
            this[k1][k2] = v.$inc;
        } else {
            this[k1][k2] += v.$inc;
        }
    } else if ( v.hasOwnProperty('$push') && Match.test( v, {$push:Match.Any} ) ) {
        if (!Match.test(this[k1][k2],Array)) {
            if (this[k1][k2]) {console.log('Warning, overwritten:',this[k1][k2]);}
            this[k1][k2] = [v.$push];
        } else {
            this[k1][k2].push(v.$push);
        }
    } else if ( v.hasOwnProperty('$pushAll') && Match.test( v, {$pushAll:[Match.Any]} ) ) {
        if (!Match.test(this[k1][k2],Array)) {
            if (this[k1][k2]) {console.log('Warning, overwritten:',this[k1][k2]);}
            this[k1][k2] = [];
        }
        this[k1][k2] = this[k1][k2].concat(v.$pushAll);
    } else if ( v.hasOwnProperty('$addByKey') && Match.test( v, {$addByKey:[Match.OneOf(Object,Array)],$key:Match.OneOf(String,Number)} ) ) {
        var sum,key=v.$key,that=this[k1][k2];
        if (!Match.test(that,Object)) {
            if (this[k1][k2]) {console.log('Warning, overwritten:',this[k1][k2]);}
            that = this[k1][k2] = {};
        }
        _.each(v.$addByKey,function(item){
            sum=that[item[key]];
            if (sum === undefined) {
                if (Match.test(item,Array)) {
                    sum=that[item[key]]=[],sum[key]=item[key];
                } else {
                    sum=that[item[key]]={},sum[key]=item[key];
                }
            }
            _.each(item,function(subItem,subKey){
                if (subKey == key) return;
                if (Match.test(subItem,Number)) {
                    if (sum[subKey] === undefined) {
                        sum[subKey] = subItem;
                    } else {
                        sum[subKey] += subItem;
                    }
                } else {
                    if (sum[subKey] === undefined) {
                        sum[subKey] = [subItem];
                    } else {
                        sum[subKey].push(subItem);
                    }
                }
            });
        });
    } else {
        this[k1][k2] = v;
    }

    if (fRun) return fRun.apply(this[k1],Array.prototype.slice.call(arguments,4));
}).bind({});

lock   = function(name){ return changeAndRun('lock',name,{$TS:true});   }
unlock = function(name){ changeAndRun('lock',name,{$set:false}); }
