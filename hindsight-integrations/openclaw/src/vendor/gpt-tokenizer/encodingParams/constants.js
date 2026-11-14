import { UNICODE_16_LETTER_RANGES, UNICODE_16_NUMBER_RANGES, UNICODE_16_O200K_LOWER_RANGES, UNICODE_16_O200K_UPPER_RANGES, UNICODE_17_TO_16_GUARD_RANGES, UNICODE_17_TO_16_LETTER_EXCLUDE_RANGES, UNICODE_17_TO_16_LETTER_INCLUDE_RANGES, UNICODE_17_TO_16_NUMBER_EXCLUDE_RANGES, UNICODE_17_TO_16_NUMBER_INCLUDE_RANGES, UNICODE_17_TO_16_O200K_LOWER_EXCLUDE_RANGES, UNICODE_17_TO_16_O200K_LOWER_INCLUDE_RANGES, UNICODE_17_TO_16_O200K_UPPER_EXCLUDE_RANGES, UNICODE_17_TO_16_O200K_UPPER_INCLUDE_RANGES } from "./unicode16Data.js";
//#region src/encodingParams/constants.ts
const nativeVersion = typeof process === "undefined" ? void 0 : process.versions?.unicode;
const unicode17Sets = (() => {
	if (nativeVersion !== "17.0") return false;
	try {
		return new RegExp("[a--b]", "v").test("a");
	} catch {
		return false;
	}
})();
const flags = unicode17Sets ? "gv" : "gu";
function category(native, stable, exclude, include) {
	if (nativeVersion === "16.0") return native;
	if (!unicode17Sets) return stable;
	const corrected = exclude ? `[[${native}]--[${exclude}]]` : `[${native}]`;
	return include ? `[${corrected}${include}]` : corrected;
}
const L = category(String.raw`\p{L}`, UNICODE_16_LETTER_RANGES, UNICODE_17_TO_16_LETTER_EXCLUDE_RANGES, UNICODE_17_TO_16_LETTER_INCLUDE_RANGES);
const N = category(String.raw`\p{N}`, UNICODE_16_NUMBER_RANGES, UNICODE_17_TO_16_NUMBER_EXCLUDE_RANGES, UNICODE_17_TO_16_NUMBER_INCLUDE_RANGES);
const UPPER = category(String.raw`\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}`, UNICODE_16_O200K_UPPER_RANGES, UNICODE_17_TO_16_O200K_UPPER_EXCLUDE_RANGES, UNICODE_17_TO_16_O200K_UPPER_INCLUDE_RANGES);
const LOWER = category(String.raw`\p{Ll}\p{Lm}\p{Lo}\p{M}`, UNICODE_16_O200K_LOWER_RANGES, UNICODE_17_TO_16_O200K_LOWER_EXCLUDE_RANGES, UNICODE_17_TO_16_O200K_LOWER_INCLUDE_RANGES);
var StableUnicodeRegExp = class extends RegExp {
	stableRegex;
	nativeRegex;
	affected;
	constructor(stablePattern, nativePattern, affected) {
		super(stablePattern, flags);
		this.stableRegex = new RegExp(stablePattern, flags);
		this.nativeRegex = new RegExp(nativePattern, "gu");
		this.affected = affected;
	}
	static get [Symbol.species]() {
		return RegExp;
	}
	[Symbol.match](text) {
		const selected = this.affected.test(text) ? this.stableRegex : this.nativeRegex;
		selected.lastIndex = this.lastIndex;
		const result = selected[Symbol.match](text);
		this.lastIndex = selected.lastIndex;
		return result;
	}
	[Symbol.matchAll](text) {
		const selected = this.affected.test(text) ? this.stableRegex : this.nativeRegex;
		selected.lastIndex = this.lastIndex;
		return selected[Symbol.matchAll](text);
	}
	exec(text) {
		const selected = this.affected.test(text) ? this.stableRegex : this.nativeRegex;
		selected.lastIndex = this.lastIndex;
		const result = selected.exec(text);
		this.lastIndex = selected.lastIndex;
		return result;
	}
};
const affected = unicode17Sets ? new RegExp(`[${UNICODE_17_TO_16_GUARD_RANGES}]`) : /\P{ASCII}/u;
const CONTRACTION_SUFFIX_PATTERN = String.raw`'(?:[sSſ]|[dD]|[mM]|[tT]|[lL][lL]|[vV][eE]|[rR][eE])`;
const OPTIONAL_CONTRACTION_SUFFIX = String.raw`(?:${CONTRACTION_SUFFIX_PATTERN})?`;
function patterns(letter, number, upper, lower) {
	return [
		String.raw`'s|'t|'re|'ve|'m|'ll|'d| ?[${letter}]+| ?[${number}]+| ?[^\p{White_Space}${letter}${number}]+|\p{White_Space}+(?!\P{White_Space})|\p{White_Space}+`,
		String.raw`${CONTRACTION_SUFFIX_PATTERN}|[^\r\n${letter}${number}]?[${letter}]+|[${number}]{1,3}| ?[^\p{White_Space}${letter}${number}]+[\r\n]*|\p{White_Space}+$|\p{White_Space}*[\r\n]|\p{White_Space}+(?!\P{White_Space})|\p{White_Space}`,
		String.raw`[^\r\n${letter}${number}]?[${upper}]*[${lower}]+${OPTIONAL_CONTRACTION_SUFFIX}|[^\r\n${letter}${number}]?[${upper}]+[${lower}]*${OPTIONAL_CONTRACTION_SUFFIX}|[${number}]{1,3}| ?[^\p{White_Space}${letter}${number}]+[\r\n\/]*|\p{White_Space}*[\r\n]+|\p{White_Space}+(?!\P{White_Space})|\p{White_Space}+`
	];
}
const native = patterns(String.raw`\p{L}`, String.raw`\p{N}`, String.raw`\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}`, String.raw`\p{Ll}\p{Lm}\p{Lo}\p{M}`);
const stable = patterns(L, N, UPPER, LOWER);
const makeRegex = (index) => nativeVersion === "16.0" ? new RegExp(native[index], "gu") : new StableUnicodeRegExp(stable[index], native[index], affected);
const R50K_TOKEN_SPLIT_REGEX = makeRegex(0);
const CL100K_TOKEN_SPLIT_REGEX = makeRegex(1);
const O200K_TOKEN_SPLIT_REGEX = makeRegex(2);
//#endregion
export { CL100K_TOKEN_SPLIT_REGEX, O200K_TOKEN_SPLIT_REGEX, R50K_TOKEN_SPLIT_REGEX };

