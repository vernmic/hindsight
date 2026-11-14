import { DEFAULT_MERGE_CACHE_SIZE } from "./constants.js";
import { CL100K_TOKEN_SPLIT_REGEX, O200K_TOKEN_SPLIT_REGEX, R50K_TOKEN_SPLIT_REGEX } from "./encodingParams/constants.js";
import { createCounter } from "./IncrementalTokenCounter.js";
import { compareUint8Arrays, isAscii, startsWithByteOrderMark, tryConvertToString } from "./utfUtil.js";
import { escapeRegExp } from "./util.js";
//#region src/BytePairEncodingCore.ts
const MAX_BATCHED_SPLIT_LENGTH = 1048576;
function isAsciiString(input) {
	for (let i = 0; i < input.length; i++) if (!isAscii(input.charCodeAt(i))) return false;
	return true;
}
const decoder = new TextDecoder("utf8", { ignoreBOM: true });
function decodeShortUtf8(word, length) {
	const a = word & 255, b = word >>> 8 & 255, c = word >>> 16 & 255, d = word >>> 24;
	if (length === 1 && a < 128) return String.fromCharCode(a);
	if (length === 2 && a >= 194 && a <= 223 && (b & 192) === 128) return String.fromCharCode((a & 31) << 6 | b & 63);
	if (length === 3 && a >= 224 && a <= 239 && (b & 192) === 128 && (c & 192) === 128) {
		const cp = (a & 15) << 12 | (b & 63) << 6 | c & 63;
		if (cp >= 2048 && (cp < 55296 || cp > 57343)) return String.fromCharCode(cp);
	}
	if (length === 4 && a >= 240 && a <= 244 && (b & 192) === 128 && (c & 192) === 128 && (d & 192) === 128) {
		const cp = (a & 7) << 18 | (b & 63) << 12 | (c & 63) << 6 | d & 63;
		if (cp >= 65536 && cp <= 1114111) return String.fromCodePoint(cp);
	}
	const bytes = new Uint8Array(length);
	for (let i = 0; i < length; i++) bytes[i] = word >>> i * 8 & 255;
	return decoder.decode(bytes);
}
var BytePairEncodingCore = class {
	mergeableBytePairRankCount;
	/**
	* an array where the index is the BPE rank,
	* and the value is the string or the array of bytes that it decodes to
	* it may contain holes if token is unused
	*/
	bytePairRankDecoder;
	bytePairNonUtfRankDecoder = /* @__PURE__ */ new Map();
	decodeByteTable;
	bytePairNonUtfSortedEncoder;
	bytePairNonUtfFirstByteOffsets = /* @__PURE__ */ new Uint32Array(257);
	singleByteRanks = (/* @__PURE__ */ new Int32Array(256)).fill(-1);
	/**
	* a reverse map of the bytePairRankDecoder,
	* where the key is the string and the value is the rank
	* values that cannot be represented as a string are present in `bytePairNonUtfSortedEncoder`.
	* Bounded merged-piece results share this map so warm encode/count needs one
	* lookup. Their arrays must be excluded from vocabulary-rank lookups.
	*/
	bytePairStringRankEncoder;
	tokenSplitRegex;
	specialTokensEncoder;
	specialTokensDecoder;
	specialTokenPatternRegex;
	textEncoder = new TextEncoder();
	mergeCache;
	mergeCacheKeys = [];
	mergeCacheHead = 0;
	mergeCacheSize;
	constructor({ bytePairRankDecoder, specialTokensEncoder, tokenSplitRegex, mergeCacheSize = DEFAULT_MERGE_CACHE_SIZE }) {
		this.bytePairRankDecoder = bytePairRankDecoder;
		this.bytePairStringRankEncoder = /* @__PURE__ */ new Map();
		this.mergeCacheSize = mergeCacheSize;
		if (mergeCacheSize > 0) this.mergeCache = /* @__PURE__ */ new Map();
		this.mergeableBytePairRankCount = Object.keys(bytePairRankDecoder).length;
		const binaryLookup = [];
		bytePairRankDecoder.forEach((value, rank) => {
			if (typeof value === "string") {
				this.bytePairStringRankEncoder.set(value, rank);
				if (value.length === 1 && value.charCodeAt(0) <= 127) this.singleByteRanks[value.charCodeAt(0)] = rank;
				return;
			}
			const byteArray = new Uint8Array(value);
			binaryLookup.push([byteArray, rank]);
			if (byteArray.length === 1) this.singleByteRanks[byteArray[0]] = rank;
			this.bytePairNonUtfRankDecoder.set(rank, byteArray);
			if (startsWithByteOrderMark(byteArray)) {
				const asString = tryConvertToString(byteArray);
				if (asString !== void 0) this.bytePairStringRankEncoder.set(asString, rank);
			}
		});
		this.bytePairNonUtfSortedEncoder = binaryLookup.sort((a, b) => compareUint8Arrays(a[0], b[0]));
		let offset = 0;
		for (let firstByte = 0; firstByte <= 256; firstByte++) {
			while (offset < binaryLookup.length && (binaryLookup[offset][0].length === 0 || binaryLookup[offset][0][0] < firstByte)) offset++;
			this.bytePairNonUtfFirstByteOffsets[firstByte] = offset;
		}
		this.specialTokensEncoder = specialTokensEncoder ?? /* @__PURE__ */ new Map();
		this.specialTokensDecoder = specialTokensEncoder ? new Map([...specialTokensEncoder].map(([key, value]) => [value, key])) : /* @__PURE__ */ new Map();
		this.tokenSplitRegex = tokenSplitRegex;
		const allSpecialTokensRegex = [...this.specialTokensEncoder.keys()].map(escapeRegExp).join("|");
		try {
			this.specialTokenPatternRegex = new RegExp(allSpecialTokensRegex, "g");
		} catch {
			throw new Error("Invalid regular expression pattern.");
		}
	}
	setMergeCacheSize(newSize) {
		if (this.mergeCacheSize === 0 && newSize > 0) this.mergeCache = /* @__PURE__ */ new Map();
		this.mergeCacheSize = newSize;
		if (this.mergeCache) {
			const oldestKeys = this.mergeCache.keys();
			while (this.mergeCache.size > newSize && this.mergeCache.size > 0) {
				const key = oldestKeys.next().value;
				this.mergeCache.delete(key);
				this.bytePairStringRankEncoder.delete(key);
			}
			this.mergeCacheKeys = [...this.mergeCache.keys()];
			this.mergeCacheHead = 0;
		}
		if (newSize === 0) {
			this.mergeCache = void 0;
			this.mergeCacheKeys = [];
			this.mergeCacheHead = 0;
		}
	}
	clearMergeCache() {
		if (this.mergeCache) {
			for (const key of this.mergeCache.keys()) this.bytePairStringRankEncoder.delete(key);
			this.mergeCache.clear();
		}
		this.mergeCacheKeys = [];
		this.mergeCacheHead = 0;
	}
	/** @internal Called with the encoding's already normalized special policy. */
	createIncrementalCounter(allowedSpecial, validate) {
		const knownPattern = [
			R50K_TOKEN_SPLIT_REGEX,
			CL100K_TOKEN_SPLIT_REGEX,
			O200K_TOKEN_SPLIT_REGEX
		].includes(this.tokenSplitRegex);
		const maxSpecialLength = Math.max(0, ...Array.from(this.specialTokensEncoder.keys(), (token) => token.length));
		return createCounter({
			scan: (text, start) => this.countNativePieces(text, start, allowedSpecial),
			canRestart: (text, offset) => knownPattern && /^[ \t\r\n][A-Za-z0-9]/.test(text.slice(offset, offset + 2)),
			validate,
			restartMargin: maxSpecialLength + 2,
			canReuseSuffix: knownPattern
		});
	}
	*countNativePieces(text, start, allowedSpecial) {
		while (start < text.length) {
			const special = this.findNextSpecialToken(text, allowedSpecial, start);
			const end = special?.[0] ?? text.length;
			const plain = text.slice(start, end);
			for (const match of plain.matchAll(this.tokenSplitRegex)) yield {
				end: start + match.index + match[0].length,
				count: this.countPiece(match[0])
			};
			if (!special) break;
			start = end + special[1].length;
			yield {
				end: start,
				count: 1
			};
		}
	}
	countPiece(piece) {
		const token = this.bytePairStringRankEncoder.get(piece);
		return typeof token === "number" ? 1 : (token ?? this.bytePairEncode(piece)).length;
	}
	*encodeNativeGenerator(text, allowedSpecial) {
		let startIndex = 0;
		let lastTokenLength = 0;
		while (true) {
			const nextSpecialMatch = this.findNextSpecialToken(text, allowedSpecial, startIndex);
			const nextSpecialStartIndex = nextSpecialMatch?.[0];
			const endIndex = nextSpecialStartIndex ?? text.length;
			const textBeforeSpecial = startIndex === 0 && endIndex === text.length ? text : text.slice(startIndex, endIndex);
			for (const [match] of textBeforeSpecial.matchAll(this.tokenSplitRegex)) {
				const token = this.getBpeRankFromString(match);
				if (token !== void 0) {
					lastTokenLength = 1;
					yield [token];
					continue;
				}
				const tokens = this.bytePairEncode(match);
				lastTokenLength = tokens.length;
				yield tokens.slice();
			}
			if (nextSpecialStartIndex !== void 0) {
				const specialToken = nextSpecialMatch[1];
				const specialTokenValue = this.specialTokensEncoder.get(specialToken);
				if (specialTokenValue === void 0) throw new Error(`Special token "${specialToken}" is not in the special token encoder.`);
				yield [specialTokenValue];
				startIndex = nextSpecialStartIndex + specialToken.length;
				lastTokenLength = 1;
			} else break;
		}
		return lastTokenLength;
	}
	encodeNative(text, allowedSpecial) {
		if (!this.tokenSplitRegex.global) throw new TypeError("The token split regular expression must be global.");
		let startIndex = 0;
		const tokensArray = [];
		while (true) {
			const nextSpecialMatch = this.findNextSpecialToken(text, allowedSpecial, startIndex);
			const nextSpecialStartIndex = nextSpecialMatch?.[0];
			const endIndex = nextSpecialStartIndex ?? text.length;
			const textBeforeSpecial = startIndex === 0 && endIndex === text.length ? text : text.slice(startIndex, endIndex);
			if (textBeforeSpecial.length > MAX_BATCHED_SPLIT_LENGTH) for (const tokens of this.encodeNativeGenerator(textBeforeSpecial)) for (let i = 0; i < tokens.length; i++) tokensArray.push(tokens[i]);
			else {
				const pieces = textBeforeSpecial.match(this.tokenSplitRegex);
				if (pieces) for (let i = 0; i < pieces.length; i++) {
					const match = pieces[i];
					const token = this.bytePairStringRankEncoder.get(match);
					if (typeof token === "number") {
						tokensArray.push(token);
						continue;
					}
					const tokens = token ?? this.bytePairEncode(match);
					for (let j = 0; j < tokens.length; j++) tokensArray.push(tokens[j]);
				}
			}
			if (nextSpecialStartIndex !== void 0) {
				const specialToken = nextSpecialMatch[1];
				const specialTokenValue = this.specialTokensEncoder.get(specialToken);
				if (specialTokenValue === void 0) throw new Error(`Special token "${specialToken}" is not in the special token encoder.`);
				tokensArray.push(specialTokenValue);
				startIndex = nextSpecialStartIndex + specialToken.length;
			} else break;
		}
		return tokensArray;
	}
	countNative(text, allowedSpecial) {
		if (!this.tokenSplitRegex.global) throw new TypeError("The token split regular expression must be global.");
		let startIndex = 0;
		let tokensCount = 0;
		while (true) {
			const nextSpecialMatch = this.findNextSpecialToken(text, allowedSpecial, startIndex);
			const nextSpecialStartIndex = nextSpecialMatch?.[0];
			const endIndex = nextSpecialStartIndex ?? text.length;
			const textBeforeSpecial = startIndex === 0 && endIndex === text.length ? text : text.slice(startIndex, endIndex);
			if (textBeforeSpecial.length > MAX_BATCHED_SPLIT_LENGTH) for (const tokens of this.encodeNativeGenerator(textBeforeSpecial)) tokensCount += tokens.length;
			else {
				const pieces = textBeforeSpecial.match(this.tokenSplitRegex);
				if (pieces) for (let i = 0; i < pieces.length; i++) {
					const match = pieces[i];
					tokensCount += this.countPiece(match);
				}
			}
			if (nextSpecialStartIndex !== void 0) {
				const specialToken = nextSpecialMatch[1];
				if (this.specialTokensEncoder.get(specialToken) === void 0) throw new Error(`Special token "${specialToken}" is not in the special token encoder.`);
				tokensCount++;
				startIndex = nextSpecialStartIndex + specialToken.length;
			} else break;
		}
		return tokensCount;
	}
	*decodeNativeGenerator(tokens) {
		for (const token of tokens) {
			const tokenBytes = this.tryDecodeToken(token);
			if (tokenBytes === void 0) throw new Error(`Token ${token} is not in the byte pair encoder.`);
			yield tokenBytes;
		}
	}
	/** Cache only bytes actually visited; keep the vocabulary's string fast path. */
	decodeByteHeavy(tokens) {
		const table = this.decodeByteTable ??= {
			bytes: /* @__PURE__ */ new Uint8Array(4096),
			offsets: new Uint32Array(this.bytePairRankDecoder.length),
			lengths: new Uint32Array(this.bytePairRankDecoder.length),
			words: new Uint32Array(this.bytePairRankDecoder.length),
			used: 0
		};
		let length = 0;
		for (let index = 0; index < tokens.length; index++) {
			const token = tokens[index];
			let size = table.lengths[token];
			if (!size) {
				const value = this.bytePairRankDecoder[token];
				if (value === void 0 || !Number.isInteger(token) || token < 0) return void 0;
				const bound = typeof value === "string" ? value.length * 3 : value.length;
				if (table.used + bound > table.bytes.length) {
					const next = new Uint8Array(Math.max(table.bytes.length * 2, table.used + bound));
					next.set(table.bytes.subarray(0, table.used));
					table.bytes = next;
				}
				table.offsets[token] = table.used;
				if (typeof value === "string") size = this.textEncoder.encodeInto(value, table.bytes.subarray(table.used)).written;
				else {
					table.bytes.set(value, table.used);
					size = value.length;
				}
				table.lengths[token] = size;
				if (size <= 4) {
					const at = table.used;
					table.words[token] = table.bytes[at] | table.bytes[at + 1] << 8 | table.bytes[at + 2] << 16 | table.bytes[at + 3] << 24;
				}
				table.used += size;
			}
			length += size;
		}
		const { bytes, offsets, lengths, words } = table;
		const output = new Uint8Array(length + 3);
		const view = new DataView(output.buffer);
		let position = 0;
		for (let index = 0; index < tokens.length; index++) {
			const token = tokens[index];
			const size = lengths[token];
			if (size > 0 && size <= 4) {
				view.setUint32(position, words[token], true);
				position += size;
			} else {
				const end = offsets[token] + size;
				for (let start = offsets[token]; start < end; start++) output[position++] = bytes[start];
			}
		}
		return decoder.decode(output.subarray(0, length));
	}
	decodeArrayByteRun(tokens, start) {
		let index = start;
		let word = 0;
		let used = 0;
		let buffer;
		for (; index < tokens.length; index++) {
			const token = tokens[index];
			const value = this.tryDecodeToken(token);
			if (value === void 0) throw new Error(`Token ${token} is not in the byte pair encoder.`);
			if (typeof value === "string") {
				if (value.length) break;
				continue;
			}
			if (used + value.length <= 4) for (let offset = 0; offset < value.length; offset++) word |= value[offset] << used++ * 8;
			else {
				if (!buffer || used + value.length > buffer.length) {
					const next = new Uint8Array(Math.max(1024, (buffer?.length ?? 0) * 2, used + value.length));
					if (used > 4) next.set(buffer.subarray(0, used));
					buffer = next;
				}
				if (used <= 4) for (let offset = 0; offset < used; offset++) buffer[offset] = word >>> offset * 8 & 255;
				buffer.set(value, used);
				used += value.length;
			}
		}
		return {
			index,
			text: used <= 4 ? decodeShortUtf8(word, used) : decoder.decode(buffer.subarray(0, used))
		};
	}
	decodeArray(tokens) {
		let decoded = "";
		for (let index = 0; index < tokens.length;) {
			const token = tokens[index];
			const value = this.tryDecodeToken(token);
			if (value === void 0) throw new Error(`Token ${token} is not in the byte pair encoder.`);
			if (typeof value === "string") {
				decoded += value;
				index++;
			} else {
				const part = this.decodeArrayByteRun(tokens, index);
				decoded += part.text;
				index = part.index;
			}
		}
		return decoded;
	}
	decodeNative(tokens) {
		if ((Array.isArray(tokens) || tokens instanceof Uint32Array) && tokens.length >= 128) {
			let binaryTokens = 0;
			const sampleLength = Math.min(tokens.length, 128);
			for (let index = 0; index < sampleLength; index++) if (typeof this.bytePairRankDecoder[tokens[index]] === "object") binaryTokens++;
			if (binaryTokens >= sampleLength / 4) {
				const decoded = this.decodeByteHeavy(tokens);
				if (decoded !== void 0) return decoded;
			}
			return this.decodeArray(tokens);
		}
		let decoded = "";
		let word = 0;
		let used = 0;
		let buffer;
		for (const token of tokens) {
			const value = this.tryDecodeToken(token);
			if (value === void 0) throw new Error(`Token ${token} is not in the byte pair encoder.`);
			if (typeof value === "string") {
				if (value.length === 0) continue;
				if (used) {
					decoded += used <= 4 ? decodeShortUtf8(word, used) : decoder.decode(buffer.subarray(0, used));
					word = 0;
					used = 0;
				}
				decoded += value;
			} else if (used + value.length <= 4) for (let index = 0; index < value.length; index++) word |= value[index] << used++ * 8;
			else {
				if (!buffer || used + value.length > buffer.length) {
					const next = new Uint8Array(Math.max(1024, (buffer?.length ?? 0) * 2, used + value.length));
					if (used > 4) next.set(buffer.subarray(0, used));
					buffer = next;
				}
				if (used <= 4) for (let index = 0; index < used; index++) buffer[index] = word >>> index * 8 & 255;
				buffer.set(value, used);
				used += value.length;
			}
		}
		if (used) decoded += used <= 4 ? decodeShortUtf8(word, used) : decoder.decode(buffer.subarray(0, used));
		return decoded;
	}
	async *decodeNativeAsyncIterable(tokens) {
		for await (const token of tokens) {
			const tokenBytesOrString = this.tryDecodeToken(token);
			if (tokenBytesOrString === void 0) throw new Error(`Token ${token} is not in the byte pair encoder.`);
			yield tokenBytesOrString;
		}
	}
	getBpeRankFromString(key) {
		const value = this.bytePairStringRankEncoder.get(key);
		return typeof value === "number" ? value : void 0;
	}
	getBpeRankFromStringOrThrow(key) {
		const value = this.getBpeRankFromString(key);
		if (value === void 0) throw new Error(`The byte-pair encoding does not contain a value for: ${key}`);
		return value;
	}
	getBpeRankFromBytes(key) {
		if (key.length === 1) {
			const rank = this.singleByteRanks[key[0]];
			return rank < 0 ? void 0 : rank;
		}
		const keyAsString = tryConvertToString(key);
		if (keyAsString !== void 0) return this.getBpeRankFromString(keyAsString);
		const index = this.binarySearch(key);
		if (index !== -1) return this.bytePairNonUtfSortedEncoder[index][1];
	}
	getBpeRankFromBytesOrThrow(key) {
		const value = this.getBpeRankFromBytes(key);
		if (value === void 0) throw new Error(`The byte-pair encoding does not contain a value for: ${key.toString()}`);
		return value;
	}
	binarySearch(key) {
		const firstByte = key[0];
		let low = this.bytePairNonUtfFirstByteOffsets[firstByte];
		let high = this.bytePairNonUtfFirstByteOffsets[firstByte + 1] - 1;
		while (low <= high) {
			const mid = low + high >>> 1;
			const midKey = this.bytePairNonUtfSortedEncoder[mid][0];
			let cmp = 0;
			const maxLength = Math.min(midKey.length, key.length);
			for (let i = 0; i < maxLength; i++) {
				cmp = midKey[i] - key[i];
				if (cmp !== 0) break;
			}
			if (cmp === 0) cmp = midKey.length - key.length;
			if (cmp === 0) return mid;
			if (cmp < 0) low = mid + 1;
			else high = mid - 1;
		}
		return -1;
	}
	findNextSpecialToken(text, allowedSpecial, startIndex) {
		if (!allowedSpecial?.size) return;
		let searchIndex = startIndex;
		while (true) {
			this.specialTokenPatternRegex.lastIndex = searchIndex;
			const nextSpecialMatch = this.specialTokenPatternRegex.exec(text);
			if (!nextSpecialMatch) return;
			const specialToken = nextSpecialMatch[0];
			if (allowedSpecial.has(specialToken)) return [nextSpecialMatch.index, specialToken];
			searchIndex = nextSpecialMatch.index + 1;
		}
	}
	tryDecodeToken(tokenRank) {
		const value = this.bytePairRankDecoder[tokenRank];
		if (typeof value === "string") return value;
		if (typeof value === "object") {
			const fromBinary = this.bytePairNonUtfRankDecoder.get(tokenRank);
			if (fromBinary) return fromBinary;
		}
		return this.specialTokensDecoder.get(tokenRank);
	}
	addToMergeCache(key, value) {
		if (!this.mergeCache) return;
		if (this.mergeCache.size > 0 && this.mergeCache.size >= this.mergeCacheSize) {
			const firstKey = this.mergeCacheKeys[this.mergeCacheHead];
			this.mergeCache.delete(firstKey);
			this.bytePairStringRankEncoder.delete(firstKey);
			this.mergeCacheKeys[this.mergeCacheHead] = key;
			this.mergeCacheHead = (this.mergeCacheHead + 1) % this.mergeCacheKeys.length;
		} else this.mergeCacheKeys.push(key);
		this.mergeCache.set(key, value);
		this.bytePairStringRankEncoder.set(key, value);
	}
	bytePairEncode(input) {
		if (input.length === 1 && isAscii(input.codePointAt(0))) return [this.getBpeRankFromStringOrThrow(input)];
		const cached = this.mergeCache?.get(input);
		if (cached !== void 0) return cached;
		const result = input.length <= 64 && isAsciiString(input) ? this.bytePairMergeAscii(input) : this.bytePairMerge(this.textEncoder.encode(input));
		this.addToMergeCache(input, result);
		return result;
	}
	bytePairMergeAscii(input) {
		const starts = [];
		const ranks = [];
		for (let i = 0; i <= input.length; i++) starts.push(i);
		const getPairRank = (index) => this.getBpeRankFromString(input.slice(starts[index], starts[index + 2])) ?? Number.POSITIVE_INFINITY;
		for (let i = 0; i < input.length - 1; i++) ranks[i] = getPairRank(i);
		while (starts.length > 2) {
			let bestRank = Number.POSITIVE_INFINITY;
			let bestIndex = -1;
			for (let i = 0; i < starts.length - 2; i++) if (ranks[i] < bestRank) {
				bestRank = ranks[i];
				bestIndex = i;
			}
			if (bestIndex === -1) break;
			starts.splice(bestIndex + 1, 1);
			ranks.splice(bestIndex + 1, 1);
			ranks[bestIndex] = bestIndex < starts.length - 2 ? getPairRank(bestIndex) : Number.POSITIVE_INFINITY;
			if (bestIndex > 0) ranks[bestIndex - 1] = getPairRank(bestIndex - 1);
		}
		const result = [];
		for (let i = 0; i < starts.length - 1; i++) result.push(this.getBpeRankFromStringOrThrow(input.slice(starts[i], starts[i + 1])));
		return result;
	}
	bytePairMerge(piece) {
		const { length } = piece;
		const next = new Int32Array(length + 1);
		const previous = new Int32Array(length + 1);
		const pairRanks = new Float64Array(length + 1);
		const live = new Uint8Array(length + 1);
		const getPairRank = (startIndex) => {
			const middle = next[startIndex];
			if (middle >= length) return Number.POSITIVE_INFINITY;
			return this.getBpeRankFromBytes(piece.subarray(startIndex, next[middle])) ?? Number.POSITIVE_INFINITY;
		};
		const heap = [];
		const isLower = (a, b) => heap[a] < heap[b] || heap[a] === heap[b] && heap[a + 1] < heap[b + 1];
		const swap = (a, b) => {
			const rank = heap[a];
			const startIndex = heap[a + 1];
			heap[a] = heap[b];
			heap[a + 1] = heap[b + 1];
			heap[b] = rank;
			heap[b + 1] = startIndex;
		};
		const push = (rank, startIndex) => {
			heap.push(rank, startIndex);
			let index = heap.length - 2;
			while (index > 0) {
				const parent = (index >> 1) - 1 >> 1 << 1;
				if (!isLower(index, parent)) break;
				swap(index, parent);
				index = parent;
			}
		};
		const pop = () => {
			const last = heap.length - 2;
			swap(0, last);
			const rank = heap[last];
			const startIndex = heap[last + 1];
			heap.length = last;
			let index = 0;
			while (true) {
				const left = index * 2 + 2;
				const right = left + 2;
				let lowest = index;
				if (left < heap.length && isLower(left, lowest)) lowest = left;
				if (right < heap.length && isLower(right, lowest)) lowest = right;
				if (lowest === index) break;
				swap(index, lowest);
				index = lowest;
			}
			return [rank, startIndex];
		};
		for (let i = 0; i <= length; i++) {
			next[i] = i + 1;
			previous[i] = i - 1;
			live[i] = 1;
		}
		for (let i = 0; i < length - 1; i++) {
			pairRanks[i] = getPairRank(i);
			if (pairRanks[i] !== Number.POSITIVE_INFINITY) push(pairRanks[i], i);
		}
		while (heap.length > 0) {
			const [rank, startIndex] = pop();
			if (!live[startIndex] || pairRanks[startIndex] !== rank) continue;
			const merged = next[startIndex];
			live[merged] = 0;
			next[startIndex] = next[merged];
			previous[next[merged]] = startIndex;
			pairRanks[startIndex] = getPairRank(startIndex);
			if (pairRanks[startIndex] !== Number.POSITIVE_INFINITY) push(pairRanks[startIndex], startIndex);
			const before = previous[startIndex];
			if (before >= 0) {
				pairRanks[before] = getPairRank(before);
				if (pairRanks[before] !== Number.POSITIVE_INFINITY) push(pairRanks[before], before);
			}
		}
		const output = [];
		for (let i = 0; i < length; i = next[i]) output.push(this.getBpeRankFromBytesOrThrow(piece.subarray(i, next[i])));
		return output;
	}
};
//#endregion
export { BytePairEncodingCore, decoder };

