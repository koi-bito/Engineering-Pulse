"""Zero-downtime Elasticsearch Reindex Migration Script.

Migrates engineering events from engineering-events-v1 to engineering-events-v2
with custom analysis pipeline (camelCase splitter, lowercase, stop words),
then atomically swaps the 'engineering-events' alias.
"""

import argparse
import asyncio
import os
import sys
import time

from elasticsearch import AsyncElasticsearch

# Ensure backend app imports are accessible
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "backend"))
from app.services.es_index import (
    ES_ALIAS,
    ES_INITIAL_INDEX,
    ES_V2_INDEX,
    INDEX_MAPPING_V2,
    INDEX_SETTINGS_V2,
)


async def run_reindex(
    es_url: str,
    source_index: str,
    dest_index: str,
    alias_name: str,
    delete_old: bool = False,
) -> None:
    print(f"Connecting to Elasticsearch at {es_url}...")
    es = AsyncElasticsearch(es_url)

    try:
        # Check cluster health
        health = await es.cluster.health()
        print(f"Cluster status: {health.get('status')}")

        # 1. Create destination index with v2 analysis & mapping if not exists
        dest_exists = await es.indices.exists(index=dest_index)
        if not dest_exists:
            print(f"Creating destination index '{dest_index}' with custom analyzer...")
            await es.indices.create(
                index=dest_index,
                settings=INDEX_SETTINGS_V2,
                mappings=INDEX_MAPPING_V2,
            )
            print(f"Created '{dest_index}'.")
        else:
            print(f"Destination index '{dest_index}' already exists.")

        # 2. Check if source index exists
        source_exists = await es.indices.exists(index=source_index)
        if not source_exists:
            print(f"Source index '{source_index}' does not exist.")
            # If alias does not exist yet, point it to dest_index
            alias_exists = await es.indices.exists_alias(name=alias_name)
            if not alias_exists:
                await es.indices.put_alias(index=dest_index, name=alias_name)
                print(f"Pointed alias '{alias_name}' directly to '{dest_index}'.")
            return

        source_count = (await es.count(index=source_index))["count"]
        print(f"Source index '{source_index}' contains {source_count} documents.")

        # 3. Execute zero-downtime reindex
        print(f"Starting reindex from '{source_index}' to '{dest_index}'...")
        start_time = time.monotonic()
        reindex_res = await es.reindex(
            source={"index": source_index},
            dest={"index": dest_index},
            wait_for_completion=True,
            refresh=True,
        )
        elapsed = time.monotonic() - start_time
        total_reindexed = reindex_res.get("total", 0)
        created = reindex_res.get("created", 0)
        updated = reindex_res.get("updated", 0)
        print(
            f"Reindex complete in {elapsed:.2f}s: total={total_reindexed}, "
            f"created={created}, updated={updated}"
        )

        dest_count = (await es.count(index=dest_index))["count"]
        print(f"Destination index '{dest_index}' now has {dest_count} documents.")

        # 4. Atomically swap alias
        print(f"Swapping alias '{alias_name}' to '{dest_index}'...")
        # Check current aliases
        alias_info = await es.indices.get_alias(name=alias_name, ignore_unavailable=True)
        actions = []
        if isinstance(alias_info, dict):
            for idx in alias_info:
                actions.append({"remove": {"index": idx, "alias": alias_name}})

        actions.append({"add": {"index": dest_index, "alias": alias_name}})
        await es.indices.update_aliases(actions=actions)
        print(f"Alias '{alias_name}' atomically pointed to '{dest_index}'.")

        # 5. Delete old index if requested
        if delete_old:
            print(f"Deleting old index '{source_index}'...")
            await es.indices.delete(index=source_index)
            print(f"Deleted '{source_index}'.")
        else:
            print(f"Old index '{source_index}' retained for rollback safety.")

        print("Zero-downtime reindexing completed successfully.")

    finally:
        await es.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Zero-downtime Elasticsearch Reindexer")
    parser.add_argument(
        "--es-url",
        default=os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"),
        help="Elasticsearch URL",
    )
    parser.add_argument(
        "--source-index",
        default=ES_INITIAL_INDEX,
        help=f"Source index (default: {ES_INITIAL_INDEX})",
    )
    parser.add_argument(
        "--dest-index",
        default=ES_V2_INDEX,
        help=f"Destination index (default: {ES_V2_INDEX})",
    )
    parser.add_argument(
        "--alias",
        default=ES_ALIAS,
        help=f"Alias name (default: {ES_ALIAS})",
    )
    parser.add_argument(
        "--delete-old",
        action="store_true",
        help="Drop source index after successful cutover",
    )
    args = parser.parse_args()

    asyncio.run(
        run_reindex(
            es_url=args.es_url,
            source_index=args.source_index,
            dest_index=args.dest_index,
            alias_name=args.alias,
            delete_old=args.delete_old,
        )
    )


if __name__ == "__main__":
    main()
