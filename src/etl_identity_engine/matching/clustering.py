"""Simple connected component clustering."""

from __future__ import annotations

from collections import defaultdict, deque


def cluster_links(links: list[tuple[str, str]]) -> list[list[str]]:
    graph: dict[str, set[str]] = defaultdict(set)
    for left, right in links:
        graph[left].add(right)
        graph[right].add(left)

    visited: set[str] = set()
    clusters: list[list[str]] = []

    for node in graph:
        if node in visited:
            continue

        queue = deque([node])
        visited.add(node)
        component: list[str] = []

        while queue:
            current = queue.popleft()
            component.append(current)
            for neighbor in graph[current]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)

        clusters.append(sorted(component))

    return sorted(clusters, key=lambda c: c[0] if c else "")


def assign_cluster_ids(
    record_ids: list[str],
    accepted_links: list[tuple[str, str]],
) -> dict[str, str]:
    linked_clusters = cluster_links(accepted_links)
    assigned = {record_id for cluster in linked_clusters for record_id in cluster}
    all_clusters = linked_clusters + [[record_id] for record_id in sorted(record_ids) if record_id not in assigned]
    ordered_clusters = sorted((sorted(cluster) for cluster in all_clusters), key=lambda cluster: cluster[0])

    cluster_ids: dict[str, str] = {}
    for index, cluster in enumerate(ordered_clusters, start=1):
        cluster_id = f"C-{index:05d}"
        for record_id in cluster:
            cluster_ids[record_id] = cluster_id

    return cluster_ids

