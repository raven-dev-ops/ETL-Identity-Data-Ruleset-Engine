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

