import type { AccountType, EntityKind } from "@myfinance/domain";

export const TRANSACTION_SEARCH_SEMANTIC_WEIGHT = 0.65;
export const TRANSACTION_SEARCH_KEYWORD_WEIGHT = 0.35;
export const TRANSACTION_SEARCH_RRF_K = 60;

export type TransactionSearchDirection = "debit" | "credit" | "neutral";
export type TransactionSearchReviewState =
  | "pending_enrichment"
  | "needs_review"
  | "resolved";

export type TransactionSearchCandidateBase = {
  transactionId: string;
  batchId: string;
  sourceBatchKey: string;
  transactionDate: string;
  postedAt: string | null;
  amount: string | null;
  currency: string | null;
  merchant: string | null;
  counterparty: string | null;
  category: string | null;
  accountId: string;
  accountName: string | null;
  institutionName: string | null;
  accountType: AccountType | null;
  economicEntityId: string | null;
  economicEntityName: string | null;
  economicEntityKind: EntityKind | null;
  direction: TransactionSearchDirection;
  reviewState: TransactionSearchReviewState;
  reviewReason: string | null;
  originalText: string;
  contextualizedText: string;
  documentSummary: string;
};

export type TransactionSemanticCandidate = TransactionSearchCandidateBase & {
  semanticDistance: number;
};

export type TransactionKeywordCandidate = TransactionSearchCandidateBase & {
  bm25Score: number;
};

export type TransactionRerankedCandidate = {
  transactionId: string;
  score: number;
};

export type FusedTransactionSearchHit = TransactionSearchCandidateBase & {
  semanticDistance: number | null;
  rerankScore: number | null;
  bm25Score: number | null;
  hybridScore: number;
  semanticRank: number | null;
  rerankRank: number | null;
  keywordRank: number | null;
  matchedBy: Array<"semantic" | "keyword">;
};

export function fuseTransactionSearchResults(input: {
  semanticCandidates: TransactionSemanticCandidate[];
  rerankedSemantic: TransactionRerankedCandidate[];
  keywordCandidates: TransactionKeywordCandidate[];
  limit?: number;
}) {
  const rows = new Map<string, FusedTransactionSearchHit>();

  input.semanticCandidates.forEach((candidate, index) => {
    rows.set(candidate.transactionId, {
      ...candidate,
      rerankScore: null,
      bm25Score: null,
      hybridScore: 0,
      semanticRank: index + 1,
      rerankRank: null,
      keywordRank: null,
      matchedBy: ["semantic"],
    });
  });

  input.keywordCandidates.forEach((candidate, index) => {
    const existing = rows.get(candidate.transactionId);
    if (existing) {
      existing.bm25Score = candidate.bm25Score;
      existing.keywordRank = index + 1;
      if (!existing.matchedBy.includes("keyword")) {
        existing.matchedBy.push("keyword");
      }
      return;
    }

    rows.set(candidate.transactionId, {
      ...candidate,
      semanticDistance: null,
      rerankScore: null,
      hybridScore: 0,
      semanticRank: null,
      rerankRank: null,
      keywordRank: index + 1,
      matchedBy: ["keyword"],
    });
  });

  input.rerankedSemantic.forEach((candidate, index) => {
    const existing = rows.get(candidate.transactionId);
    if (!existing) {
      return;
    }

    existing.rerankScore = candidate.score;
    existing.rerankRank = index + 1;
    existing.hybridScore +=
      TRANSACTION_SEARCH_SEMANTIC_WEIGHT /
      (TRANSACTION_SEARCH_RRF_K + index + 1);
  });

  input.keywordCandidates.forEach((candidate, index) => {
    const existing = rows.get(candidate.transactionId);
    if (!existing) {
      return;
    }

    existing.hybridScore +=
      TRANSACTION_SEARCH_KEYWORD_WEIGHT /
      (TRANSACTION_SEARCH_RRF_K + index + 1);
  });

  return [...rows.values()]
    .sort((left, right) => {
      if (right.hybridScore !== left.hybridScore) {
        return right.hybridScore - left.hybridScore;
      }

      return left.transactionId.localeCompare(right.transactionId);
    })
    .slice(0, input.limit ?? 8);
}
