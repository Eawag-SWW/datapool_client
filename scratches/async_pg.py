import asyncio
import psycopg2
import time

connection_details = {
    "host": "eaw-sdwh3.eawag.wroot.emp-eaw.ch",
    "user": "datapool",
    "port": 5432,
    "database": "datapool",
    "password": "corona666"
}

# start for start_timestamp; end for end_timestamp
sql = """
    WITH 
    site_ids AS (SELECT site_id::integer FROM site WHERE site.name = '164_luppmenweg')
    SELECT signal.timestamp, value, parameter.unit, parameter.name, source.name, source.serial, source_type.name, site.name  
        FROM signal
        INNER JOIN site ON signal.site_id = site.site_id
        INNER JOIN parameter ON signal.parameter_id = parameter.parameter_id
        INNER JOIN source ON signal.source_id = source.source_id
        INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
        WHERE 
        signal.site_id = ANY(ARRAY(SELECT site_id::integer FROM site_ids))
    AND '{start}'::timestamp <= signal.timestamp
    AND signal.timestamp <= '{end}'::timestamp 
    ORDER BY signal.timestamp ASC;
"""


def query(conn, sql):
    conn = psycopg2.connect(
        host=conn["host"],
        port=conn["port"],
        database=conn["database"],
        user=conn["user"],
        password=conn["password"],
    )
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data


async def query_async(conn, sql):
    return await asyncio.to_thread(query, conn, sql)


async def run_query(conn, sql):
    dates = [
        ("2021-01-01 00:00:00", "2022-01-01 00:00:00"),
    ]
    results = await asyncio.gather(*[query_async(conn, sql.format(start=start, end=end)) for start, end in dates])
    return results


async def run_query_asnc(conn, sql):
    dates = [
        ("2021-01-01 00:00:00", "2021-06-01 00:00:00"),
        ("2021-06-01 00:00:00", "2022-01-01 00:00:00"),
    ]
    results = await asyncio.gather(*[query_async(conn, sql.format(start=start, end=end)) for start, end in dates])
    return results


st = time.time()
res = asyncio.run(run_query(connection_details, sql))
print(time.time()-st)
del res
st = time.time()
res = asyncio.run(run_query_asnc(connection_details, sql))
print(time.time()-st)
