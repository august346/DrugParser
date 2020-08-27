import json

import asyncpg

from pharmacy.editor import extract_o3_data


async def pharmacy_inserting(src_data_queue, rule_key):
    connect = await asyncpg.connect(
        user='postgres',
        password='postgres',
        database='pharmacy',
        host='127.0.0.1',
        port=5432
    )
    await _get_rule(rule_key)(src_data_queue, connect)
    await connect.close()


def _get_rule(key):
    return {
        'o3': _o3_insert_process
    }.get(key)


async def _pg_fetch(connect, query):
    return await connect.fetch(query)


async def _o3_insert_process(queue, connect):
    await _pg_fetch(connect, _o3_create_table_query())
    while (data_batch := await queue.get()) is not None:
        await _pg_fetch(connect, _o3_insert_batch_query(map(extract_o3_data, data_batch)))


def _o3_create_table_query():
    with open('src/o3_create_table.sql', 'r', encoding='utf-8') as file:
        return '\n'.join(file.readlines())


def _o3_insert_batch_query(batch):
    return 'INSERT INTO o3_pharmacy VALUES {};'.format(
        ', '.join(map(
            lambda x: '(\'{}\')'.format(json.dumps(x, ensure_ascii=False).replace('\'', '`')),
            batch
        ))
    )
