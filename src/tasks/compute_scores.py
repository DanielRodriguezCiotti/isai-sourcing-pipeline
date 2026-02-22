"""
Compute solution_fit_cg and solution_fit_by scores using 1-nearest-neighbor
on pre-computed solution_and_use_cases_embedding vectors.

For each target domain, find the closest manually-labeled reference company
(by cosine similarity / dot product on L2-normalized embeddings) and assign
its manual score.
"""

import json

import numpy as np
from prefect import task
from prefect.tasks import exponential_backoff

from src.config.clients import get_supabase_client
from src.utils.db import fetch_in_batches, upsert_in_batches
from src.utils.logger import get_logger


def _parse_embedding(raw) -> np.ndarray | None:
    """Parse a pgvector embedding returned by PostgREST."""
    if raw is None:
        return None
    if isinstance(raw, list):
        return np.array(raw, dtype=np.float32)
    # string format from pgvector: "[0.1,0.2,...]"
    return np.array(json.loads(raw), dtype=np.float32)


@task(
    name="compute_scores",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=4),
)
def compute_scores(domains: list[str]):
    logger = get_logger()
    client = get_supabase_client()

    logger.info(f"Starting compute_scores for {len(domains)} domains")

    # Step 1 – Fetch reference companies (those with manual scores)
    ref_rows = (
        client.table("companies")
        .select("domain, solution_fit_cg_manual, solution_fit_by_manual")
        .or_("solution_fit_cg_manual.not.is.null,solution_fit_by_manual.not.is.null")
        .execute()
        .data
    )
    logger.info(f"Fetched {len(ref_rows)} reference companies with manual scores")

    if not ref_rows:
        logger.warning("No reference companies found — skipping score computation")
        return

    # Step 2 – Collect all domains needing embeddings
    ref_domains = {r["domain"] for r in ref_rows}
    all_domains = list(ref_domains | set(domains))

    # Step 3 – Fetch embeddings
    embedding_rows = fetch_in_batches(
        client,
        "company_embeddings",
        "domain",
        all_domains,
        select="domain, solution_and_use_cases_embedding",
    )
    embeddings_map: dict[str, np.ndarray] = {}
    for row in embedding_rows:
        vec = _parse_embedding(row["solution_and_use_cases_embedding"])
        if vec is not None:
            embeddings_map[row["domain"]] = vec

    logger.info(f"Fetched {len(embeddings_map)} embeddings")

    # Step 4 – Build reference matrices (one per score type)
    ref_cg_domains, ref_cg_scores, ref_cg_vecs = [], [], []
    ref_by_domains, ref_by_scores, ref_by_vecs = [], [], []

    for row in ref_rows:
        d = row["domain"]
        if d not in embeddings_map:
            continue
        vec = embeddings_map[d]
        if row["solution_fit_cg_manual"] is not None:
            ref_cg_domains.append(d)
            ref_cg_scores.append(row["solution_fit_cg_manual"])
            ref_cg_vecs.append(vec)
        if row["solution_fit_by_manual"] is not None:
            ref_by_domains.append(d)
            ref_by_scores.append(row["solution_fit_by_manual"])
            ref_by_vecs.append(vec)

    logger.info(
        f"Reference pool: {len(ref_cg_vecs)} CG refs, {len(ref_by_vecs)} BY refs"
    )

    # Step 5 – Build target matrix
    target_domains = [d for d in domains if d in embeddings_map]
    missing_domains = [d for d in domains if d not in embeddings_map]

    if missing_domains:
        logger.warning(
            f"{len(missing_domains)} domains have no embedding — scores will be None"
        )

    records = []

    if target_domains:
        target_matrix = np.stack([embeddings_map[d] for d in target_domains])

        # Pre-build reference matrices once
        ref_cg_matrix = np.stack(ref_cg_vecs) if ref_cg_vecs else None
        ref_by_matrix = np.stack(ref_by_vecs) if ref_by_vecs else None

        # Step 6 – Compute similarity & assign scores (vectorized)
        if ref_cg_matrix is not None:
            sim_cg = target_matrix @ ref_cg_matrix.T  # (n_targets, n_refs_cg)
            best_cg_indices = np.argmax(sim_cg, axis=1)
        if ref_by_matrix is not None:
            sim_by = target_matrix @ ref_by_matrix.T  # (n_targets, n_refs_by)
            best_by_indices = np.argmax(sim_by, axis=1)

        for i, domain in enumerate(target_domains):
            rec = {"domain": domain, "solution_fit_cg": None, "solution_fit_by": None}
            if ref_cg_matrix is not None:
                rec["solution_fit_cg"] = int(ref_cg_scores[best_cg_indices[i]])
            if ref_by_matrix is not None:
                rec["solution_fit_by"] = int(ref_by_scores[best_by_indices[i]])
            records.append(rec)

    # Domains without embeddings get None for both scores
    for domain in missing_domains:
        records.append(
            {"domain": domain, "solution_fit_cg": None, "solution_fit_by": None}
        )

    # Step 7 – Upsert
    if records:
        upsert_in_batches(
            client,
            "business_computed_values",
            records,
            on_conflict="domain",
            logger=logger,
        )
        logger.info(f"Upserted {len(records)} score records")
    else:
        logger.info("No records to upsert")

    logger.info("compute_scores complete")
