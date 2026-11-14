//#region src/IncrementalTokenCounter.ts
function upperBound(pieces, offset) {
	let low = 0;
	let high = pieces.length;
	while (low < high) {
		const mid = low + high >>> 1;
		if (pieces[mid].end <= offset) low = mid + 1;
		else high = mid;
	}
	return low;
}
/** @internal Tokenizer-owned callbacks keep regex and special policy here opaque. */
function createCounter({ scan, canRestart, validate, restartMargin, canReuseSuffix }) {
	let text = "";
	const pieces = [];
	let total = 0;
	const applyEdit = (start, deleteCount, inserted) => {
		if (!Number.isSafeInteger(start) || !Number.isSafeInteger(deleteCount) || start < 0 || deleteCount < 0 || start > text.length || deleteCount > text.length - start) throw new RangeError("The edit range must lie within the tracked text.");
		if (deleteCount === 0 && inserted.length === 0) return total;
		const next = text.slice(0, start) + inserted + text.slice(start + deleteCount);
		validate(next, start, inserted.length);
		let keep = upperBound(pieces, start - restartMargin);
		while (keep > 0 && !canRestart(text, pieces[keep - 1].end)) keep--;
		const anchor = keep > 0 ? pieces[keep - 1].end : 0;
		const delta = inserted.length - deleteCount;
		const replacement = [];
		let resume = pieces.length;
		for (const piece of scan(next, anchor)) {
			replacement.push(piece);
			const oldEnd = piece.end - delta;
			if (!canReuseSuffix || piece.end < start + inserted.length || oldEnd < start + deleteCount) continue;
			const index = upperBound(pieces, oldEnd) - 1;
			if (index >= keep && pieces[index].end === oldEnd) {
				resume = index + 1;
				break;
			}
		}
		let nextTotal = total;
		for (let i = keep; i < resume; i++) nextTotal -= pieces[i].count;
		for (const piece of replacement) nextTotal += piece.count;
		const oldLength = pieces.length;
		const suffixStart = keep + replacement.length;
		const nextLength = oldLength + replacement.length - (resume - keep);
		if (nextLength > oldLength) pieces.length = nextLength;
		if (suffixStart !== resume) pieces.copyWithin(suffixStart, resume, oldLength);
		pieces.length = nextLength;
		for (let i = 0; i < replacement.length; i++) pieces[keep + i] = replacement[i];
		if (delta !== 0) for (let i = suffixStart; i < pieces.length; i++) pieces[i].end += delta;
		text = next;
		total = nextTotal;
		return total;
	};
	return {
		applyEdit,
		append: (inserted) => applyEdit(text.length, 0, inserted),
		update(next) {
			let start = 0;
			const shared = Math.min(text.length, next.length);
			while (start < shared && text.charCodeAt(start) === next.charCodeAt(start)) start++;
			let oldEnd = text.length;
			let newEnd = next.length;
			while (oldEnd > start && newEnd > start && text.charCodeAt(oldEnd - 1) === next.charCodeAt(newEnd - 1)) {
				oldEnd--;
				newEnd--;
			}
			return applyEdit(start, oldEnd - start, next.slice(start, newEnd));
		},
		count: () => total,
		reset() {
			text = "";
			pieces.length = 0;
			total = 0;
		}
	};
}
//#endregion
export { createCounter };

