var base58 = require('base58-native');
var cnUtil = require('cryptoforknote-util');

exports.uid = function(){
    var min = 1000000000;
    var max = 1999999999;
    var id = Math.floor(Math.random() * (max - min + 1)) + min;
    return id.toString();
};

exports.ringBuffer = function(maxSize){
    var data = [];
    var cursor = 0;
    var isFull = false;

    return {
        append: function(x){
            if (isFull){
                data[cursor] = x;
                cursor = (cursor + 1) % maxSize;
            }
            else{
                data.push(x);
                cursor++;
                if (data.length === maxSize){
                    cursor = 0;
                    isFull = true;
                }
            }
        },
        avg: function(plusOne){
            var sum = data.reduce(function(a, b){ return a + b }, plusOne || 0);
            return sum / ((isFull ? maxSize : cursor) + (plusOne ? 1 : 0));
        },
        size: function(){
            return isFull ? maxSize : cursor;
        },
        clear: function(){
            data = [];
            cursor = 0;
            isFull = false;
        }
    };
};

var addressBase58Prefix = parseInt(cnUtil.address_decode(Buffer.from(config.poolServer.poolAddress)).toString());
var integratedAddressBase58Prefix = config.poolServer.walletBase58SubAddressPrefix ? parseInt(config.poolServer.walletBase58SubAddressPrefix) : addressBase58Prefix + 1;
var subAddressBase58Prefix = config.poolServer.walletBase58SubAddressPrefix? parseInt(config.poolServer.walletBase58SubAddressPrefix) : "N/A";

var cnUtil = require('cryptoforknote-util');
exports.cnUtil = cnUtil;

exports.validateMinerAddress = function(address) {
    var addressPrefix = getAddressPrefix(address);
    if (addressPrefix === addressBase58Prefix) return true;
    else if (addressPrefix === integratedAddressBase58Prefix) return true;
    else if (addressPrefix === subAddressBase58Prefix) return true;
    return false;
}

function getAddressPrefix(address) {
    var addressBuffer = Buffer.from(address);

    var addressPrefix = cnUtil.address_decode(addressBuffer);
    if (addressPrefix) addressPrefix = parseInt(addressPrefix.toString());

    if (!addressPrefix) {
        addressPrefix = cnUtil.address_decode_integrated(addressBuffer);
        if (addressPrefix) addressPrefix = parseInt(addressPrefix.toString());
    }

    return addressPrefix || null;
}
exports.getAddressPrefix = getAddressPrefix;

function cleanupSpecialChars(str) {
    str = str.replace(/[ÀÁÂÃÄÅ]/g,"A");
    str = str.replace(/[àáâãäå]/g,"a");
    str = str.replace(/[ÈÉÊË]/g,"E");
    str = str.replace(/[èéêë]/g,"e");
    str = str.replace(/[ÌÎÏ]/g,"I");
    str = str.replace(/[ìîï]/g,"i");
    str = str.replace(/[ÒÔÖ]/g,"O");
    str = str.replace(/[òôö]/g,"o");
    str = str.replace(/[ÙÛÜ]/g,"U");
    str = str.replace(/[ùûü]/g,"u");
    return str.replace(/[^A-Za-z0-9\-\_]/gi,'');
}
exports.cleanupSpecialChars = cleanupSpecialChars;



exports.varIntEncode = function(n){

};
