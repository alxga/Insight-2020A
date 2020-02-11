"""Query strings"""

__author__ = "Alex Ganin"


Queries = {
"countApprox": """
SELECT table_rows "Rows Count"
FROM information_schema.tables
WHERE table_name='%s'
;
""",

"countWhere": """
SELECT count(*)
FROM `%s`
WHERE %s
;
""",
}
