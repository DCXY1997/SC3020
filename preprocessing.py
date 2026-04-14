import psycopg2

#edit your db settings
def get_connection(host="localhost", port=5432, dbname="TPC-H", user="postgres", password="sudo"):

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    conn.autocommit = True
    return conn


def test_connection(host="localhost", port=5432, dbname="TPC-H",
                    user="postgres", password="sudo") -> bool:
    try:
        conn = get_connection(host, port, dbname, user, password)
        conn.close()
        return True
    except Exception:
        return False

def get_qep(conn, query: str) -> dict:
    explain_query = f"EXPLAIN (FORMAT JSON, ANALYZE FALSE, VERBOSE FALSE) {query}"
    with conn.cursor() as cur:
        cur.execute(explain_query)
        result = cur.fetchone()
    return result[0][0]


def set_planner_options(conn, settings: list):
    with conn.cursor() as cur:
        for setting, value in settings:
            cur.execute(f"SET {setting} = {value};")


def reset_planner_options(conn):
    with conn.cursor() as cur:
        for name, diable, enable, label in POSTGRE_SETTINGS:
            cur.execute(f"SET {name} = {enable};")


def get_aqps(conn, query: str) -> list:
    aqps = []
    join_scan = FORCE_JOIN + FORCE_SCAN

    for join_scan_conditions in join_scan:
        label_parts = [f"{s}={v}" for s, v in join_scan_conditions]
        label = " | ".join(label_parts)
        try:
            set_planner_options(conn, join_scan_conditions)
            plan = get_qep(conn, query)
            aqps.append({"label": label, "plan": plan})
        except Exception as e:
            aqps.append({"label": label, "plan": None, "error": str(e)})
        finally:
            reset_planner_options(conn)

    return aqps


def get_plan_nodes(plan_dict: dict) -> list:
    nodes = []

    def append_child_node(node):
        if not isinstance(node, dict):
            return
        nodes.append(node)
        for child in node.get("Plans", []):
            append_child_node(child)

    root = plan_dict.get("Plan", plan_dict)
    append_child_node(root)
    return nodes


def get_plan_cost(plan_dict: dict) -> float:
    root = plan_dict.get("Plan", plan_dict)
    return float(root.get("Total Cost", 0.0))


def get_table_stats(conn, table_name: str) -> dict:
    stats = {"row_estimate": None, "has_index": False, "index_names": []}

    with conn.cursor() as cur:
        cur.execute(
            "SELECT reltuples FROM pg_class WHERE relname = %s;", #gets the row count estimate for a table and pg_class is a system catelog table that stores the metadata
            (table_name.lower(),)
        )
        row = cur.fetchone()
        if row:
            stats["row_estimate"] = int(row[0])

    with conn.cursor() as cur:
        cur.execute(
            "SELECT indexname FROM pg_indexes WHERE tablename = %s;",
            (table_name.lower(),)
        )
        indexes = cur.fetchall()
        stats["index_names"] = [r[0] for r in indexes]
        stats["has_index"] = len(stats["index_names"]) > 0

    return stats


def get_all_table_stats(conn, tables: list) -> dict:
    return {t: get_table_stats(conn, t) for t in tables}

POSTGRE_SETTINGS = [
    ("enable_hashjoin",   "off", "on", "Hash Join disabled"),
    ("enable_mergejoin",  "off", "on", "Merge Join disabled"),
    ("enable_nestloop",   "off", "on", "Nested Loop disabled"),
    ("enable_seqscan",    "off", "on", "Sequential Scan disabled"),
    ("enable_indexscan",  "off", "on", "Index Scan disabled"),
    ("enable_bitmapscan", "off", "on", "Bitmap Scan disabled"),
]

FORCE_JOIN = [
    [("enable_hashjoin", "off"), ("enable_mergejoin", "off")], #force NL join
    [("enable_hashjoin", "off"), ("enable_nestloop", "off")], #foece merge join
    [("enable_mergejoin", "off"), ("enable_nestloop", "off")], #force hash join
]

FORCE_SCAN = [
    [("enable_seqscan", "off"), ("enable_bitmapscan", "off")], #force index scan
    [("enable_indexscan", "off"), ("enable_bitmapscan", "off")], #force seq scan
    [("enable_seqscan", "off"), ("enable_indexscan", "off")], #force bitmap scan
]
