"""Entity-relationship graph built on networkx for traversal and analysis."""

from typing import Optional

import networkx as nx

from spark_pdm_generator.models.logical import (
    Cardinality,
    Entity,
    LogicalModel,
    Relationship,
)


class ERGraph:
    """Directed graph representing entity-relationship model.

    Nodes are entities. Edges are relationships (parent -> child).
    Edge attributes store cardinality, key columns, and identifying flag.
    """

    def __init__(self) -> None:
        self._graph = nx.DiGraph()

    @classmethod
    def from_logical_model(cls, model: LogicalModel) -> "ERGraph":
        """Build the ER graph from a parsed logical model."""
        graph = cls()
        for entity in model.entities:
            graph._graph.add_node(
                entity.entity_name,
                entity=entity,
                attribute_count=len(model.get_attributes_for_entity(entity.entity_name)),
            )
        for rel in model.relationships:
            graph._graph.add_edge(
                rel.parent_entity,
                rel.child_entity,
                relationship=rel,
                cardinality=rel.cardinality,
                parent_key_columns=rel.parent_key_columns,
                child_key_columns=rel.child_key_columns,
                is_identifying=rel.is_identifying,
            )
        return graph

    def get_entity(self, name: str) -> Optional[Entity]:
        """Get the Entity object for a node."""
        if name in self._graph.nodes:
            return self._graph.nodes[name].get("entity")
        return None

    def get_parents(self, entity_name: str) -> list[str]:
        """Get parent entities (entities that this entity references via FK)."""
        if entity_name not in self._graph:
            return []
        return list(self._graph.predecessors(entity_name))

    def get_children(self, entity_name: str) -> list[str]:
        """Get child entities (entities that reference this entity)."""
        if entity_name not in self._graph:
            return []
        return list(self._graph.successors(entity_name))

    def get_relationship(
        self, parent: str, child: str
    ) -> Optional[Relationship]:
        """Get the relationship between two entities."""
        if self._graph.has_edge(parent, child):
            return self._graph.edges[parent, child].get("relationship")
        return None

    def get_one_to_one_parents(self, entity_name: str) -> list[str]:
        """Get parents with 1:1 cardinality (merge candidates)."""
        parents = []
        for parent in self.get_parents(entity_name):
            rel = self.get_relationship(parent, entity_name)
            if rel and rel.cardinality == Cardinality.ONE_TO_ONE:
                parents.append(parent)
        return parents

    def get_small_dimension_parents(
        self, entity_name: str, model: LogicalModel, threshold: int
    ) -> list[str]:
        """Get parent dimensions small enough to denormalize.

        Returns parents where:
        - Cardinality is 1:N (parent is the "one" side)
        - Parent estimated_row_count < threshold
        """
        parents = []
        for parent in self.get_parents(entity_name):
            rel = self.get_relationship(parent, entity_name)
            if rel and rel.cardinality == Cardinality.ONE_TO_MANY:
                row_count = model.get_entity_row_count(parent)
                if row_count is not None and row_count < threshold:
                    parents.append(parent)
        return parents

    def get_large_dimension_parents(
        self, entity_name: str, model: LogicalModel, threshold: int
    ) -> list[str]:
        """Get parent dimensions too large to denormalize."""
        parents = []
        for parent in self.get_parents(entity_name):
            rel = self.get_relationship(parent, entity_name)
            if rel and rel.cardinality == Cardinality.ONE_TO_MANY:
                row_count = model.get_entity_row_count(parent)
                if row_count is None or row_count >= threshold:
                    parents.append(parent)
        return parents

    def get_all_dimension_parents(self, entity_name: str) -> list[str]:
        """Get ALL parent dimensions with 1:N cardinality regardless of size.

        Used by aggressive denormalization mode to absorb every parent.
        """
        parents = []
        for parent in self.get_parents(entity_name):
            rel = self.get_relationship(parent, entity_name)
            if rel and rel.cardinality == Cardinality.ONE_TO_MANY:
                parents.append(parent)
        return parents

    def get_composition_children(self, entity_name: str) -> list[str]:
        """Get child entities linked by composition (embedded arrays).

        These are children where is_identifying=True, meaning the child
        data is embedded in the parent (e.g., MongoDB subdocuments).
        Always candidates for denormalization regardless of mode.
        """
        children = []
        for child in self.get_children(entity_name):
            edge_data = self._graph.edges[entity_name, child]
            if edge_data.get("is_identifying", False):
                children.append(child)
        return children

    def is_self_referencing(self, entity_name: str) -> bool:
        """Check if entity has a self-referencing FK (hierarchy)."""
        return self._graph.has_edge(entity_name, entity_name)

    def get_many_to_many_relationships(
        self, entity_name: str
    ) -> list[tuple[str, Relationship]]:
        """Get M:N relationships involving this entity."""
        results = []
        for parent in self.get_parents(entity_name):
            rel = self.get_relationship(parent, entity_name)
            if rel and rel.cardinality == Cardinality.MANY_TO_MANY:
                results.append((parent, rel))
        for child in self.get_children(entity_name):
            rel = self.get_relationship(entity_name, child)
            if rel and rel.cardinality == Cardinality.MANY_TO_MANY:
                results.append((child, rel))
        return results

    def get_all_entity_names(self) -> list[str]:
        """Get all entity names in the graph."""
        return list(self._graph.nodes)

    def get_attribute_count(self, entity_name: str) -> int:
        """Get the attribute count stored on the node."""
        if entity_name in self._graph.nodes:
            return self._graph.nodes[entity_name].get("attribute_count", 0)
        return 0

    def in_degree(self, entity_name: str) -> int:
        """Number of parents (incoming edges)."""
        if entity_name not in self._graph:
            return 0
        return self._graph.in_degree(entity_name)

    def out_degree(self, entity_name: str) -> int:
        """Number of children (outgoing edges)."""
        if entity_name not in self._graph:
            return 0
        return self._graph.out_degree(entity_name)
