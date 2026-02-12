import re
from typing import Any, Dict, List

from cleanco import basename
from rapidfuzz import fuzz, process
from tqdm import tqdm
from unidecode import unidecode


class CompanyFuzzyMatcher:
    def __init__(self, references: List[str], scorer=fuzz.ratio, threshold: int = 90):
        """
        Initializes the matcher and pre-processes the reference list for speed.

        :param references: List of company names to match against (the "Database").
        :param scorer: RapidFuzz scoring function (default: ratio).
        :param threshold: Minimum score (0-100) to consider a match valid.
        """
        self.original_references = references
        self.scorer = scorer
        self.threshold = threshold
        self.clean_references = [self._preprocess(ref) for ref in references]

    @staticmethod
    def _preprocess(name: str) -> str:
        """
        Static helper to clean company names.
        1. Remove legal entities (Inc, LLC, etc.) via cleanco.
        2. Remove special chars/punctuation.
        3. Normalize case and whitespace.
        4. Replace accents with their non-accented equivalents.
        """
        if not name or not isinstance(name, str):
            return ""

        # 1. Use cleanco to strip legal suffixes (basename matches "Apple Inc" -> "Apple")
        # cleanco works best on Title Case or original case, so we do this before lowercasing
        clean_name = basename(name)

        # 2. Lowercase and standard whitespace cleanup
        clean_name = clean_name.lower().strip()

        # 3. Remove punctuation (dots, commas, dashes) to merge "micro-soft" and "microsoft"
        clean_name = re.sub(r"[^\w\s]", "", clean_name)

        # 4. Collapse multiple spaces
        clean_name = " ".join(clean_name.split())

        # 5. Replace accents with their non-accented equivalents
        clean_name = unidecode(clean_name)

        return clean_name

    def match_single(self, input_name: str) -> Dict[str, Any]:
        """
        Matches a single input string against the pre-processed references.
        """
        clean_input = self._preprocess(input_name)

        # Edge case: If input became empty after cleaning (e.g. input was just "Inc."), return nothing
        if not clean_input:
            return {"input": input_name, "match": None, "score": 0, "index": -1}

        # RapidFuzz extractOne
        # Returns: (matched_string, score, index_in_list)
        result = process.extractOne(
            clean_input, self.clean_references, scorer=self.scorer
        )

        match_str, score, idx = result

        # Threshold check
        if score >= self.threshold:
            return {
                "input": input_name,
                "match": self.original_references[
                    idx
                ],  # Return the original reference name
                "clean_match": match_str,  # The version used for scoring
                "score": round(score, 2),
                "index": idx,
            }
        else:
            return {
                "input": input_name,
                "match": None,
                "score": round(score, 2),
                "index": -1,
            }

    def match_batch(self, inputs: List[str]) -> List[Dict[str, Any]]:
        """
        Process a list of inputs efficiently.
        """
        results = []
        for name in tqdm(inputs, desc="Matching batch"):
            results.append(self.match_single(name))
        return results


# ==========================================
# USAGE EXAMPLE
# ==========================================

if __name__ == "__main__":
    # 1. Setup Data (2000 references vs 1000 inputs simulation)
    # Note: 'cleanco' handles variations like 'Limited', 'GmbH', 'S.A.'
    with open("global_2000.txt", "r") as f:
        refs = f.readlines()
    refs = list(set([ref.strip() for ref in refs]))

    with open("clients.txt", "r") as f:
        inputs = f.readlines()
    inputs = list(set([input.strip() for input in inputs]))

    # 2. Initialize Matcher
    # Scorer Recommendation:
    # - fuzz.token_set_ratio: Best generic choice (handles "Apple" vs "Apple Computers" well)
    # - fuzz.ratio: Use if you want strict equality (only allowing small typos)
    matcher = CompanyFuzzyMatcher(references=refs, scorer=fuzz.ratio, threshold=85)

    # 3. Run Batch
    results = matcher.match_batch(inputs)

    # 4. Display Results
    import pandas as pd

    df = pd.DataFrame(results)

    # Save only matches to csv sorted by score descending
    df[df["match"].notnull()].sort_values(by="score", ascending=False).to_csv(
        "matches_bis.csv", index=False
    )
    print("Saved matches to matches.csv")
