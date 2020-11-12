const os = require('os');
const fs = require('fs');
const path = require('path');

const utils = module.exports = {
    get platform() {
        let platform = os.platform();
        platform = ({ 'win32' : 'windows' }[platform]) || platform;
        let arch = os.arch();
        return `${platform}-${arch}`;
    }
}

const toString = o=>Object.prototype.toString.call(o);
utils.toString = toString;

utils.isString = o=>toString(o)=='[object String]';
utils.isObject = o=>toString(o)=='[object Object]';
if(Array.isArray)
    utils.isArray = o=>Array.isArray(o);
else
    utils.isArray = o=>toString(o)=='[object Array]';


utils.each = (obj, iteratee)=>{

    if(utils.isObject(obj)){
        let keys = Object.keys(obj);
        for (let i = 0, length = keys.length; i < length; i++) {
            iteratee(obj[keys[i]], keys[i], obj);
        }
        return
    }
    if(typeof obj.forEach == 'function'){
        obj.forEach(iteratee);
        return
    }

    let length = obj && obj.length;
    if(typeof length == 'number'){
        for (let i = 0; i < length; i++) {
            iteratee(obj[i], i, obj);
        }
        return;
    }

    return false;
}

utils.sortBy = (list, iteratee)=>{
    return list.map((value, index)=>{
        return {
            value,
            index,
            criteria: iteratee(value, index, array)
        };
    }).sort((left, right)=>{
        let a = left.criteria;
        let b = right.criteria;
        if (a !== b) {
            if (a > b || a === void 0) return 1;
            if (a < b || b === void 0) return -1;
        }
        return left.index - right.index;
    }).map(v=>v.value);
}

utils.merge = (dst, ...sources)=>{
    sources.forEach((src)=>{
        utils.each(src, (v, k)=>{
            if(utils.isArray(v)){
                dst[k] = [];
                utils.merge(dst[k], v);
            }else if(utils.isObject(v)) {
                if(!dst[k] || utils.isString(dst[k]) || !utils.isObject(dst[k]))
                    dst[k] = { };
                utils.merge(dst[k], v);
            }else{
                if(utils.isArray(src))
                    dst.push(v);
                else
                    dst[k] = v;
            }
        })
    })
    return dst;
}

utils.getConfig = (name, defaults = null)=>{
    let data = [ ];
    [name, name+'.'+os.hostname(), name+'.local'].forEach((filename) => {
        if(fs.existsSync(filename)) 
            data.push(fs.readFileSync(filename) || null);
    })

    if(!data[0] && !data[1]) {
        return defaults;
    }

    let o = defaults || { }
    data.forEach((conf) => {
        if(!conf || !conf.toString('utf-8').length)
            return;
        let layer = eval('('+conf.toString('utf-8')+')');
        utils.merge(o, layer);
    })

    return o;
}