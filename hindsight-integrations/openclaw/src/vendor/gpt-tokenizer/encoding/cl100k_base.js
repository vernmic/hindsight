import { ALL_SPECIAL_TOKENS, DEFAULT_MERGE_CACHE_SIZE } from "../constants.js";
import { EndOfPrompt, EndOfText, FimMiddle, FimPrefix, FimSuffix, HarmonyCall, HarmonyChannel, HarmonyConstrain, HarmonyEnd, HarmonyMessage, HarmonyReturn, HarmonyStart, HarmonyStartOfText, ImEnd, ImSep, ImStart } from "../specialTokens.js";
import { GptEncoding } from "../GptEncoding.js";
import bpe from "../bpeRanks/cl100k_base.js";
//#region src/encoding/cl100k_base.ts
const api = GptEncoding.getEncodingApi("cl100k_base", () => bpe);
const { decode, decodeAsyncGenerator, decodeGenerator, encode, encodeGenerator, isWithinTokenLimit, countTokens, createIncrementalCounter, encodeChat, encodeChatGenerator, vocabularySize, setMergeCacheSize, clearMergeCache, estimateCost } = api;
//#endregion
export { ALL_SPECIAL_TOKENS, DEFAULT_MERGE_CACHE_SIZE, EndOfPrompt, EndOfText, FimMiddle, FimPrefix, FimSuffix, HarmonyCall, HarmonyChannel, HarmonyConstrain, HarmonyEnd, HarmonyMessage, HarmonyReturn, HarmonyStart, HarmonyStartOfText, ImEnd, ImSep, ImStart, clearMergeCache, countTokens, createIncrementalCounter, decode, decodeAsyncGenerator, decodeGenerator, api as default, encode, encodeChat, encodeChatGenerator, encodeGenerator, estimateCost, isWithinTokenLimit, setMergeCacheSize, vocabularySize };

