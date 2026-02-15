from prefect import task

from src.config.clients import get_embedding_model, get_supabase_client
from src.utils.db import fetch_in_batches, keep_latest_per_domain, upsert_in_batches
from src.utils.logger import get_logger

EMBED_BATCH_SIZE = 100

DIMENSIONS = [
    ("description", "description_embedding"),
    ("detailed_solution", "detailed_solution_embedding"),
    ("solution_and_use_cases", "solution_and_use_cases_embedding"),
]


def _build_text(record: dict, dimension: str) -> str | None:
    """Build the text input for a given dimension, returning None if empty."""
    if dimension == "solution_and_use_cases":
        solution = record.get("detailed_solution")
        use_cases = record.get("use_cases")
        parts = [p for p in (solution, use_cases) if p]
        return "\n".join(parts) if parts else None
    value = record.get(dimension)
    return value if value else None


@task(name="embed_textual_dimensions")
def embed_textual_dimensions(domains: list[str]):
    logger = get_logger()
    client = get_supabase_client()
    embedder = get_embedding_model()

    # 1. Fetch and deduplicate
    records = fetch_in_batches(client, "web_scraping_enrichment", "domain", domains)
    records = keep_latest_per_domain(records)
    records = [r for r in records if r.get("description") is not None]
    logger.info(f"Found {len(records)} records with descriptions to embed")

    if not records:
        return

    # 2. Collect all texts to embed in a flat list, tracking their origin
    texts: list[str] = []
    index_map: list[tuple[int, str]] = []  # (record_idx, embedding_field)

    for rec_idx, record in enumerate(records):
        for src_field, emb_field in DIMENSIONS:
            text = _build_text(record, src_field)
            if text:
                index_map.append((rec_idx, emb_field))
                texts.append(text)

    logger.info(f"Embedding {len(texts)} texts across {len(records)} records")

    # 3. Embed in chunks
    all_vectors: list[list[float]] = []
    for i in range(0, len(texts), EMBED_BATCH_SIZE):
        chunk = texts[i : i + EMBED_BATCH_SIZE]
        vectors = embedder(chunk)
        all_vectors.extend(vectors)
        logger.info(
            f"Embedded chunk {i // EMBED_BATCH_SIZE + 1}/"
            f"{(len(texts) - 1) // EMBED_BATCH_SIZE + 1}"
        )

    # 4. Map vectors back to records
    upsert_records: dict[int, dict] = {}
    for vec, (rec_idx, emb_field) in zip(all_vectors, index_map):
        if rec_idx not in upsert_records:
            upsert_records[rec_idx] = {"domain": records[rec_idx]["domain"]}
        upsert_records[rec_idx][emb_field] = vec

    rows = list(upsert_records.values())
    logger.info(f"Upserting {len(rows)} embedding records")

    # 5. Upsert
    upsert_in_batches(
        client, "company_embeddings", rows, on_conflict="domain", logger=logger
    )


if __name__ == "__main__":
    with open("by_scoring_domains_filtered.txt", "r") as f:
        by_scoring_domains = f.read().splitlines()
    with open("cap_scoring_domains_filtered.txt", "r") as f:
        cap_scoring_domains = f.read().splitlines()
    all_domains = list(set(by_scoring_domains + cap_scoring_domains))
    print(f"Number of domains in all_domains: {len(all_domains)}")
    embed_textual_dimensions(all_domains)
