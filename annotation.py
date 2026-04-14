import re
from dataclasses import dataclass, field
from typing import Optional

from preprocessing import get_plan_nodes, get_plan_cost

@dataclass
class Annotation:
  sql_cat:str
  sql_fragment:str
  operator:str
  reason:str
  cost_qep:float = 0.0
  cost_alts:dict  = field(default_factory=dict)
  node:dict  = field(default_factory=dict)


sql_tables = re.compile(
    r'\b(FROM|JOIN)\s+([\w\.]+)\s*(?:AS\s+)?([\w]*)',
    re.IGNORECASE
)


def get_tables_from_sql(query: str) -> dict:

    tables = {}

    for match in sql_tables.finditer(query):
      table_name = match.group(2).split(".")[-1].lower()
      alias = match.group(3).strip().lower()
      tables[table_name] = table_name

      if alias:
        tables[alias] = table_name

    return tables


def get_join_conditions(query: str) -> list:

    conditions = []

    on_pattern = re.compile(
        r'\bON\b\s+(.+?)(?=\bWHERE\b|\bJOIN\b|\bGROUP\b|\bORDER\b|\bHAVING\b|\bLIMIT\b|$)',
        re.IGNORECASE | re.DOTALL
    )

    for m in on_pattern.finditer(query):
        conditions.append(m.group(1).strip())

    where_pattern = re.compile(
      r'\bWHERE\b(.+?)(?=\bGROUP\b|\bORDER\b|\bHAVING\b|\bLIMIT\b|$)',
      re.IGNORECASE | re.DOTALL
    )

    for m in where_pattern.finditer(query):

        where_body = m.group(1)
        join_condition_re = re.compile(r'[\w\.]+\s*=\s*[\w\.]+')

        for jc in join_condition_re.findall(where_body):

          parts = [p.strip() for p in jc.split("=")]
          lhs_table = parts[0].split(".")[0].lower() if "." in parts[0] else None
          rhs_table = parts[1].split(".")[0].lower() if "." in parts[1] else None

          if lhs_table and rhs_table and lhs_table != rhs_table:
              conditions.append(jc.strip())

    return conditions


def has_order_by(query: str) -> bool:

    return bool(re.search(r'\bORDER\s+BY\b', query, re.IGNORECASE))

def has_group_by(query: str) -> bool:

    return bool(re.search(r'\bGROUP\s+BY\b', query, re.IGNORECASE))


def has_aggregate(query: str) -> bool:

    return bool(re.search(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', query, re.IGNORECASE))


def has_subquery(query: str) -> bool:

  return bool(re.search(r'\(\s*SELECT\b', query, re.IGNORECASE))


def has_limit(query: str) -> bool:

    return bool(re.search(r'\bLIMIT\b', query, re.IGNORECASE))

NODE_TYPE_NAMES = {
    "Seq Scan":          "Sequential Scan",
    "Index Scan":        "Index Scan",
    "Index Only Scan":   "Index-Only Scan",
    "Bitmap Heap Scan":  "Bitmap Heap Scan",
    "Bitmap Index Scan": "Bitmap Index Scan",
    "Hash Join":  "Hash Join",
    "Merge Join":        "Merge Join",
    "Nested Loop":  "Nested Loop Join",
    "Hash":              "Hash (build phase)",
    "Sort":  "Sort",
    "Incremental Sort":  "Incremental Sort",
    "Aggregate":         "Aggregate",
    "Hash Aggregate":  "Hash Aggregate",
    "Group Aggregate": "Group Aggregate",
    "Limit":   "Limit",
    "Subquery Scan":     "Subquery Scan",
    "Materialize":  "Materialize",
    "Unique":   "Unique",
    "Append":            "Append",
    "Gather":   "Gather (parallel)",
    "Gather Merge":      "Gather Merge (parallel)",
}

def explain_scan_node(node: dict, table_stats: dict) -> str:

  node_type = node.get("Node Type", "")
  relation  = node.get("Relation Name", "").lower()
  stats = table_stats.get(relation, {})
  has_index = stats.get("has_index", False)
  index_names = stats.get("index_names", [])

  if node_type == "Seq Scan":

      if not has_index:
          return f"Sequential scan is used on '{relation}' because no index exists on the table. A full table scan is the only option."

      else:
        return f"Sequential scan is used on '{relation}' despite existing indexes ({', '.join(index_names)}). The planner estimated that scanning the entire table is cheaper because a large fraction of rows are needed."

  elif node_type in ("Index Scan", "Index Only Scan"):

      index  = node.get("Index Name", "unknown index")
      condition = node.get("Index condition", "")
      reason = f"Index scan uses '{index}' on '{relation}' to quickly locate matching rows without reading the full table."

      if condition:
        reason += f" Index condition: {condition}."

      return reason

  elif node_type in ("Bitmap Heap Scan", "Bitmap Index Scan"):

    index = node.get("Index Name", "an index")
    return f"Bitmap scan on '{relation}' uses '{index}' to collect matching row pointers into a bitmap, then fetches them in heap order. This is efficient when many rows match the condition."

  return f"{NODE_TYPE_NAMES.get(node_type, node_type)} is used to access '{relation}'."


def explain_join_node(node: dict, qep_cost: float, aqp_costs: dict) -> str:

    node_type  = node.get("Node Type", "")
    join_type  = node.get("Join Type", "Inner")
    condition  = (node.get("Hash condition") or node.get("Merge condition") or node.get("Join Filter", ""))
    opertor_name = NODE_TYPE_NAMES.get(node_type, node_type)
    reason  = f"{opertor_name} is used for this {join_type.lower()} join"

    if condition:
        reason += f" on condition {condition}"

    reason += "."

    comparisons = []

    for alt_op, alt_cost in aqp_costs.items():

        if alt_op == node_type or alt_cost is None:
          continue

        if qep_cost > 0:
          ratio    = alt_cost / qep_cost
          alt_name = NODE_TYPE_NAMES.get(alt_op, alt_op)
          comparisons.append(f"{alt_name} would cost ~{ratio:.1f}x more")

    if comparisons:
        reason += " Compared to alternatives: " + "; ".join(comparisons) + "."

    if node_type == "Hash Join":
        reason += " Hash join builds a hash table on the smaller relation and probes it with the larger one. This is efficient for large unsorted inputs with equality conditions."

    elif node_type == "Merge Join":
      reason += " Merge join requires both inputs to be sorted on the join key — efficient when inputs are already ordered or small enough to sort cheaply."

    elif node_type == "Nested Loop":
        reason += " Nested loop iterates over each row of the outer relation and looks up matching rows in the inner relation — efficient when the inner relation is small or has an index on the join key."

    return reason


def explain_sort_node(node: dict, query: str) -> str:

  sort_keys = node.get("Sort Key", [])
  keys_str = ", ".join(sort_keys) if sort_keys else "unknown keys"
  reason    = f"A sort operation is performed on [{keys_str}]."

  if has_order_by(query):
      reason += " This directly corresponds to the ORDER BY clause in your query."

  else:
      reason += " This sort is introduced by the planner to satisfy a downstream operation (e.g., merge join or grouping) that requires ordered input."

  return reason


def explain_aggregate_node(node: dict, query: str) -> str:

    node_type = node.get("Node Type", "Aggregate")
    strategy  = node.get("Strategy", "")
    keys = node.get("Group Key", [])
    keys_str  = ", ".join(keys) if keys else ""

    if node_type == "Hash Aggregate" or strategy == "Hashed":
      reason = "Hash aggregate groups rows by building an in-memory hash table on the group keys."

      if keys_str:
          reason += f" Group keys: {keys_str}."
      reason += " This is efficient when the number of distinct groups fits in memory."

    elif node_type == "Group Aggregate" or strategy == "Sorted":
        reason = "Group aggregate processes rows in sorted order — it requires pre-sorted input on the group keys."

        if keys_str:
          reason += f" Group keys: {keys_str}."

    else:

      reason = f"Aggregate operation ({NODE_TYPE_NAMES.get(node_type, node_type)}) computes summary values."

      if keys_str:
          reason += f" Group keys: {keys_str}."

    if has_aggregate(query):
        reason += " This corresponds to the aggregate function(s) in your SELECT clause."

    if has_group_by(query):
        reason += " This also satisfies the GROUP BY clause."

    return reason


def explain_limit_node(node: dict) -> str:
    return "A Limit node truncates the result set to the number specified in the LIMIT clause, stopping further processing once enough rows are produced."


def explain_subquery_node(node: dict) -> str:
  return "A subquery scan materialises the result of a subquery (nested SELECT) before the outer query can process it."


def explain_materialize_node(node: dict) -> str:
    return "A Materialize node caches its input in memory so that it can be re-scanned multiple times (e.g., for the inner side of a nested-loop join) without re-executing it."

class AnnotationEngine:

    def __init__(self, qep: dict, aqps: list, table_stats: dict, query: str):
        self.qep = qep
        self.aqps = aqps
        self.table_stats = table_stats
        self.query = query
        self.annotations = []
        self._aqp_join_costs = self.compute_aqp_join_costs()

    def compute_aqp_join_costs(self) -> dict:
        join_costs = {}
        for aqp_entry in self.aqps:
            plan = aqp_entry.get("plan")
            if plan is None:
                continue
            label = aqp_entry.get("label", "")
            root_cost = get_plan_cost(plan)   #total cost of entire AQP plan

            if "enable_nestloop=off" in label and "enable_hashjoin=off" in label:
                join_costs["Merge Join"] = root_cost
            elif "enable_nestloop=off" in label and "enable_mergejoin=off" in label:
                join_costs["Hash Join"] = root_cost
            elif "enable_hashjoin=off" in label and "enable_mergejoin=off" in label:
                join_costs["Nested Loop"] = root_cost

        return join_costs

    def annotate_node(self, node: dict) -> Optional[Annotation]:
        node_type = node.get("Node Type", "")
        cost = node.get("Total Cost", 0.0)
        relation  = node.get("Relation Name", "")

        if node_type in ("Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan", "Bitmap Index Scan"):
            sql_frag = self.find_table_fragment(relation)
            reason = explain_scan_node(node, self.table_stats)
            return Annotation(
                sql_cat="SCAN",
                sql_fragment=sql_frag,
                operator=NODE_TYPE_NAMES.get(node_type, node_type),
                reason=reason,
                cost_qep=cost,
                node=node,
            )


        elif node_type in ("Hash Join", "Merge Join", "Nested Loop"):
          sql_frag  = self.find_join_fragment()
          alt_costs = {k: v for k, v in self._aqp_join_costs.items() if k != node_type}
          reason    = explain_join_node(node, cost, alt_costs)
          return Annotation(
              sql_cat="JOIN",
              sql_fragment=sql_frag,
              operator=NODE_TYPE_NAMES.get(node_type, node_type),
              reason=reason,
              cost_qep=cost,
              cost_alts=alt_costs,
              node=node,
          )


        elif node_type in ("Sort", "Incremental Sort"):
            sql_frag = self.find_clause_fragment("ORDER BY")
            reason   = explain_sort_node(node, self.query)
            return Annotation(
                sql_cat="SORT",
                sql_fragment=sql_frag,
                operator=NODE_TYPE_NAMES.get(node_type, node_type),
                reason=reason,
                cost_qep=cost,
                node=node,
            )

        elif node_type in ("Aggregate", "Hash Aggregate", "Group Aggregate"):
          sql_frag = self._find_aggregate_fragment()
          reason = explain_aggregate_node(node, self.query)
          return Annotation(
              sql_cat="AGGREGATE",
              sql_fragment=sql_frag,
              operator=NODE_TYPE_NAMES.get(node_type, node_type),
              reason=reason,
              cost_qep=cost,
              node=node,
          )

        elif node_type == "Limit":
            sql_frag = self.find_clause_fragment("LIMIT")
            reason = explain_limit_node(node)
            return Annotation(
                sql_cat="LIMIT",
                sql_fragment=sql_frag,
                operator="Limit",
                reason=reason,
                cost_qep=cost,
                node=node,
            )

        elif node_type == "Subquery Scan":
            return Annotation(
              sql_cat="SUBQUERY",
              sql_fragment="(SELECT ...)",
              operator="Subquery Scan",
              reason=explain_subquery_node(node),
              cost_qep=cost,
              node=node,
            )

        elif node_type == "Materialize":
          return Annotation(
              sql_cat="MATERIALIZE",
              sql_fragment="",
              operator="Materialize",
              reason=explain_materialize_node(node),
              cost_qep=cost,
              node=node,
          )

        return None


    def find_table_fragment(self, relation: str) -> str:
        if not relation:
          return ""
        
        pattern = re.compile(r'\b' + re.escape(relation) + r'\b', re.IGNORECASE) # Example SQL Statement SELECT * FROM customer where c_custkey = 1, and this only capture From customer where"
        m = pattern.search(self.query)

        if m:
          start = max(0, m.start() - 5) #start 5 character before m
          end   = min(len(self.query), m.end() + 10) #end 10 characters after m
          return self.query[start:end].strip()
        
        return relation

    def find_join_fragment(self) -> str:
      on_pattern = re.compile(
          r'\b((?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+OUTER\s+|CROSS\s+)?JOIN\b.+?\bON\b.+?)'
          r'(?=\b(?:WHERE|GROUP|ORDER|LIMIT|JOIN|$))',
          re.IGNORECASE | re.DOTALL)
      m = on_pattern.search(self.query)
      if m:
          return m.group(1).strip()[:120]

      where_pattern = re.compile(
          r'\bWHERE\b(.+?)(?=\bGROUP\b|\bORDER\b|\bHAVING\b|\bLIMIT\b|$)',
          re.IGNORECASE | re.DOTALL)
      m = where_pattern.search(self.query)
      if m:
          return ("WHERE " + m.group(1).strip())[:120]

      return "JOIN condition"

    def find_clause_fragment(self, keyword: str) -> str:
        pattern = re.compile(r'\b' + keyword + r'\b.{0,80}', re.IGNORECASE | re.DOTALL)
        m = pattern.search(self.query)
        return m.group(0).strip() if m else keyword

    def _find_aggregate_fragment(self) -> str:
      agg_pattern = re.compile(
          r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\([^)]*\)', re.IGNORECASE)
      matches = agg_pattern.findall(self.query)
      if matches:
          return ", ".join(matches)
      return self.find_clause_fragment("GROUP BY")


    def annotate(self) -> list:
        nodes = get_plan_nodes(self.qep) #get dict of QEP tree using DFS
        seen  = {}

        for node in nodes:
          ann = self.annotate_node(node)
          if ann is None:
              continue
          key = (ann.sql_cat, ann.operator)
          if key not in seen or ann.cost_qep > seen[key].cost_qep: #get more expensive opertor
              seen[key] = ann

        self.annotations = list(seen.values())
        return self.annotations


def format_annotation(ann: Annotation) -> str:
    lines = [
        f"[{ann.sql_cat}] {ann.operator}",
        f"  SQL: {ann.sql_fragment}",
        f"  Reason: {ann.reason}",
        f"  QEP cost: {ann.cost_qep:.2f}",
    ]
    if ann.cost_alts:
      alts = "; ".join(
          f"{NODE_TYPE_NAMES.get(k, k)}: {v:.2f}"
          for k, v in ann.cost_alts.items()
      )
      lines.append(f"  Alternative costs: {alts}")
    return "\n".join(lines)
