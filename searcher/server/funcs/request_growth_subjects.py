from datetime import date

from server.funcs.oracle_subjects import filter_subjects_list


def merge_subjects_list(file_subject_id: int, product_subjects: list[int]) -> list[int]:
    filtered = filter_subjects_list(product_subjects) or []
    subjects_set = set(filtered)
    if file_subject_id > 0:
        subjects_set.add(file_subject_id)
    return sorted(subjects_set)


async def fetch_product_subjects_by_queries(
    client,
    query_ids: tuple[int, ...],
    target_date: date,
) -> dict[int, list[int]]:
    if not query_ids:
        return {}

    stmt = """
        SELECT
            rp.query AS query_id,
            groupArray(rp.subject_id) AS subjects
        FROM request_product AS rp
        WHERE rp.city = 1
          AND rp.date = (SELECT id FROM dates WHERE date = %(v_date)s LIMIT 1)
          AND rp.query IN %(v1)s
          AND rp.place <= 300
          AND rp.subject_id > 0
        GROUP BY rp.query
    """
    result = await client.query(stmt, parameters={"v1": query_ids, "v_date": target_date})
    return {row[0]: row[1] for row in result.result_rows}


async def attach_subjects_list_to_growth_rows(
    growth_rows: list[tuple],
    client,
) -> list[tuple]:
    if not growth_rows:
        return growth_rows

    target_date: date = growth_rows[0][1]
    query_ids = tuple(sorted({row[0] for row in growth_rows}))
    step = 1000
    product_subjects_by_query: dict[int, list[int]] = {}

    for i in range(0, len(query_ids), step):
        query_part = query_ids[i : i + step]
        if not query_part:
            continue
        batch_subjects = await fetch_product_subjects_by_queries(
            client=client,
            query_ids=query_part,
            target_date=target_date,
        )
        product_subjects_by_query.update(batch_subjects)

    enriched_rows = []
    for row in growth_rows:
        query_id = int(row[0])
        file_subject_id = int(row[8])
        product_subjects = product_subjects_by_query.get(query_id, [])
        subjects_list = merge_subjects_list(file_subject_id, product_subjects)
        enriched_rows.append((*row, subjects_list))

    return enriched_rows
