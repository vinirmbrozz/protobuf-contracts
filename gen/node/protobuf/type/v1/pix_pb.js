// source: protobuf/type/v1/pix.proto
/**
 * @fileoverview
 * @enhanceable
 * @suppress {missingRequire} reports error on implicit type usages.
 * @suppress {messageConventions} JS Compiler reports an error if a variable or
 *     field starts with 'MSG_' and isn't a translatable message.
 * @public
 */
// GENERATED CODE -- DO NOT EDIT!
/* eslint-disable */
// @ts-nocheck

var jspb = require('google-protobuf');
var goog = jspb;
var global =
    (typeof globalThis !== 'undefined' && globalThis) ||
    (typeof window !== 'undefined' && window) ||
    (typeof global !== 'undefined' && global) ||
    (typeof self !== 'undefined' && self) ||
    (function () { return this; }).call(null) ||
    Function('return this')();

goog.exportSymbol('proto.protobuf.type.v1.PixKeyType', null, global);
/**
 * @enum {number}
 */
proto.protobuf.type.v1.PixKeyType = {
  PIX_KEY_TYPE_UNSPECIFIED: 0,
  PIX_KEY_TYPE_CPF: 1,
  PIX_KEY_TYPE_CNPJ: 2,
  PIX_KEY_TYPE_EMAIL: 3,
  PIX_KEY_TYPE_PHONE: 4,
  PIX_KEY_TYPE_RANDOM: 5
};

goog.object.extend(exports, proto.protobuf.type.v1);
