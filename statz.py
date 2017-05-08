#!/usr/bin/python
import argparse
import time
import sys
import psycopg2

parser = argparse.ArgumentParser(description="Description goes here")
parser.add_argument('-c','--connection', default="dbname=postgres", help="""Connection string for use by psycopg. Defaults to "dbname=postgres" (local socket connecting to postgres database). dbname parameter in connection string is required.""")
parser.add_argument('-i', '--interval', default=5, help="Interval value in seconds. Default is 5 seconds.")
parser.add_argument('-t', '--total_duration', default=600, help="Total duration in seconds to run script. Default is 10 minutes (600 seconds).")
parser.add_argument('--debug', action="store_true", help="Show additional debugging output")
args = parser.parse_args()


def conn_init():
    global dbname
    dbname_found = False
    for c in args.connection.split(" "):
        if args.debug:
            print("connection paramter: " + str(c))
        if c.find("dbname=") != -1:
            dbname = c.split("=")[1]
            dbname_found = True
            break
    if not dbname_found:
        print("Missing dbname parameter in database connection string")
        sys.exit(2)

    if args.debug:
        print "Connecting to database   -> %s" % (dbname)

    global conn
    conn = psycopg2.connect(args.connection)
#
global fduration
fduration=float(args.total_duration)
#
global finterval
finterval=float(args.interval)

## not using files but i'm keeping this as an example
#def file_read():
#    file = open(sql_file, 'r')
#    sqlFile = file.read()
#    file.close()
#    global sqlCommands
#    sqlCommands = sqlFile.split(';')

def schema_init():
    print "Initializing schema.."
    init_statz = """
            begin ;
            drop schema if exists statz cascade;
            create schema if not exists statz;
            create table if not exists statz.index_activity as select now()::timestamp without time zone as snap_date, * from pg_stat_user_indexes limit 0;
            create table if not exists statz.backend_activity as select now()::timestamp without time zone as snap_date,* from pg_stat_activity limit 0;
            create table if not exists statz.lock_activity as select now()::timestamp without time zone as snap_date,* from pg_locks limit 0;
            create table if not exists statz.table_activity as select now()::timestamp without time zone as snap_date,* from pg_stat_user_tables limit 0;
            create table if not exists statz.database_activity as select now()::timestamp without time zone as snap_date,* from pg_stat_database limit 0;
            create table if not exists statz.bgwriter_activity as select now()::timestamp without time zone as snap_date,* from pg_stat_bgwriter limit 0;
            CREATE TABLE If not exists statz.database_activity_agg (
                snap_date timestamp without time zone,
                "interval" interval,
                step interval,
                datname name,
                commits bigint,
                rows_returned bigint,
                rows_fetched bigint,
                rows_inserted bigint,
                rows_updated bigint,
                rows_deleted bigint,
                blocks_read bigint,
                blocks_hit_cached bigint,
                commits_per_sec bigint,
                rows_returned_per_sec bigint,
                rows_fetched_per_sec bigint,
                rows_inserted_per_sec bigint,
                rows_updated_per_sec bigint,
                rows_deleted_per_sec bigint,
                blocks_read_per_sec bigint,
                blocks_hit_cached_per_sec bigint,
                txn_per_sec bigint,
                cache_hit_ratio numeric
            );
            CREATE TABLE If not exists statz.table_activity_agg (
                snap_date timestamp without time zone,
                "interval" interval,
                step interval,
                table_name text,
                seq_scans bigint,
                seq_rows_read bigint,
                index_scans bigint,
                index_rows_fetched bigint,
                rows_inserted bigint,
                rows_updated bigint,
                rows_deleted bigint,
                rows_hot_updated bigint,
                live_row_count bigint,
                n_dead_tup bigint,
                seq_scans_per_sec bigint,
                seq_rows_read_per_sec bigint,
                index_scans_per_sec bigint,
                index_rows_fetched_per_sec bigint,
                rows_inserted_per_sec bigint,
                rows_updated_per_sec bigint,
                rows_deleted_per_sec bigint,
                rows_hot_updated_per_sec bigint,
                live_row_count_per_sec bigint,
                n_dead_tup_per_sec bigint
                );
            create view statz.table_stats_per_sec as
                    select snap_date,
                    step,
                    table_name,
                    seq_scans_per_sec,
                    seq_rows_read_per_sec,
                    index_scans_per_sec,
                    index_rows_fetched_per_sec,
                    rows_inserted_per_sec,
                    rows_updated_per_sec,
                    rows_deleted_per_sec,
                    rows_hot_updated_per_sec from statz.table_activity_agg;
            create view statz.db_stats_per_sec as
                    select snap_date,
                    step ,
                    commits_per_sec ,
                    rows_returned_per_sec ,
                    rows_fetched_per_sec ,
                    rows_inserted_per_sec ,
                    rows_updated_per_sec ,
                    rows_deleted_per_sec ,
                    blocks_read_per_sec ,
                    blocks_hit_cached_per_sec ,
                    txn_per_sec from statz.database_activity_agg ;

            commit ;"""
    cursor = conn.cursor()
    cursor.execute(init_statz)


def main_task():
    print "gathering statz.."
    all_statz_gather_sql = """
            insert into statz.backend_activity select now()::timestamp(0),* from pg_stat_activity;
            insert into statz.lock_activity select now()::timestamp(0),* from pg_locks;
            insert into statz.table_activity select now()::timestamp(0),* from pg_stat_user_tables;
            insert into statz.index_activity select now()::timestamp(0),* from pg_stat_user_indexes;
            insert into statz.database_activity select now()::timestamp(0),* from pg_stat_database where datname = '{0}' ;
            insert into statz.bgwriter_activity select now()::timestamp(0),* from pg_stat_bgwriter;
            Commit;""".format(dbname)
    cursor = conn.cursor()
    cursor.execute(all_statz_gather_sql)

def db_statz():
    stat_db_sql = """
        insert into statz.database_activity_agg select
        snap_date,
        snap_date-LAG(snap_date, 1, snap_date) OVER (ORDER BY snap_date) AS interval,
        snap_date - (select min (snap_date) from statz.database_activity) as step,
        datname,
        xact_commit - LAG(xact_commit, 1, xact_commit) OVER (ORDER BY snap_date) as commits,
        tup_returned - LAG(tup_returned, 1, tup_returned) OVER (ORDER BY snap_date) as rows_returned,
        tup_fetched - LAG(tup_fetched, 1, tup_fetched) OVER (ORDER BY snap_date)  as rows_fetched,
        tup_inserted - LAG(tup_inserted, 1, tup_inserted) OVER (ORDER BY snap_date) as rows_inserted,
        tup_updated - LAG(tup_updated, 1, tup_updated) OVER (ORDER BY snap_date) as rows_updated,
        tup_deleted - LAG(tup_deleted, 1, tup_deleted) OVER (ORDER BY snap_date) as rows_deleted,
        blks_read - LAG(blks_read, 1, blks_read) OVER (ORDER BY snap_date) as blocks_read,
        blks_hit - LAG(blks_hit, 1, blks_hit) OVER (ORDER BY snap_date) as blocks_hit_cached,
        (xact_commit - LAG(xact_commit, 1, xact_commit) OVER (ORDER BY snap_date) ) / {0} as commits_per_sec,
        (tup_returned - LAG(tup_returned, 1, tup_returned) OVER (ORDER BY snap_date)) / {0} as rows_returned_per_sec,
        (tup_fetched - LAG(tup_fetched, 1, tup_fetched) OVER (ORDER BY snap_date)) / {0} as rows_fetched_per_sec,
        (tup_inserted - LAG(tup_inserted, 1, tup_inserted) OVER (ORDER BY snap_date)) / {0} as rows_inserted_per_sec,
        (tup_updated - LAG(tup_updated, 1, tup_updated) OVER (ORDER BY snap_date)) / {0} as rows_updated_per_sec,
        (tup_deleted - LAG(tup_deleted, 1, tup_deleted) OVER (ORDER BY snap_date)) / {0} as rows_deleted_per_sec,
        (blks_read - LAG(blks_read, 1, blks_read) OVER (ORDER BY snap_date)) / {0} as blocks_read_per_sec,
        (blks_hit - LAG(blks_hit, 1, blks_hit) OVER (ORDER BY snap_date)) / {0} as blocks_hit_cached_per_sec,
        (( xact_commit + xact_rollback ) - (LAG(xact_commit, 1, xact_commit) OVER (ORDER BY snap_date) +
        LAG(xact_rollback, 1, xact_rollback) OVER (ORDER BY snap_date))) / {0} as txn_per_sec,
        round(CAST (((blks_hit - LAG(blks_hit, 1, blks_hit) OVER (ORDER BY snap_date) )::real /
        ((blks_hit - LAG(blks_hit, 1, blks_hit) OVER (ORDER BY snap_date)) +
        (blks_read - LAG(blks_read, 1, blks_read) OVER (ORDER BY snap_date) )+0.001)) * 100 as numeric ),4) as cache_hit_ratio
        FROM statz.database_activity where snap_date in (select snap_date from statz.database_activity order by snap_date desc limit 2) limit 1 offset 1 ;
        commit;
        """.format(finterval)
    cursor = conn.cursor()
    cursor.execute(stat_db_sql)

def table_statz():
    table_statz_sql = """
    insert into statz.table_activity_agg select *
        from (select
        snap_date,
        snap_date-LAG(snap_date, 1, snap_date) OVER (ORDER BY relid,snap_date) AS interval,
        snap_date - (select min (snap_date) from statz.table_activity) as step,
        schemaname||'.'||relname as table_name,
        coalesce( seq_scan - LAG(seq_scan, 1, seq_scan) OVER (ORDER BY relid,snap_date) ,'0') as seq_scans,
    	coalesce( seq_tup_read - LAG(seq_tup_read, 1, seq_tup_read) OVER (ORDER BY relid,snap_date) ,'0') as seq_rows_read ,
    	coalesce( idx_scan - LAG(idx_scan, 1, idx_scan) OVER (ORDER BY relid,snap_date) ,'0') as index_scans,
    	coalesce( idx_tup_fetch - LAG(idx_tup_fetch, 1, idx_tup_fetch) OVER (ORDER BY relid,snap_date) ,'0') as index_rows_fetched,
    	coalesce( n_tup_ins - LAG(n_tup_ins, 1, n_tup_ins) OVER (ORDER BY relid,snap_date) ,'0') as rows_inserted,
    	coalesce( n_tup_upd - LAG(n_tup_upd, 1, n_tup_upd) OVER (ORDER BY relid,snap_date) ,'0') as rows_updated,
    	coalesce( n_tup_del - LAG(n_tup_del, 1, n_tup_del) OVER (ORDER BY relid,snap_date) ,'0') as rows_deleted,
    	coalesce( n_tup_hot_upd - LAG(n_tup_hot_upd, 1, n_tup_hot_upd) OVER (ORDER BY relid,snap_date) ,'0') as rows_hot_updated,
    	coalesce( n_live_tup - LAG(n_live_tup, 1, n_live_tup) OVER (ORDER BY relid,snap_date) ,'0') as live_row_count,
    	coalesce( n_dead_tup - LAG(n_dead_tup, 1, n_dead_tup) OVER (ORDER BY relid,snap_date) ,'0') as n_dead_tup,
    	coalesce( seq_scan - LAG(seq_scan, 1, seq_scan) OVER (ORDER BY relid,snap_date) ,'0') / {0} as seq_scans_per_sec,
    	coalesce( seq_tup_read - LAG(seq_tup_read, 1, seq_tup_read) OVER (ORDER BY relid,snap_date) ,'0') / {0} as seq_rows_read_per_sec ,
    	coalesce( idx_scan - LAG(idx_scan, 1, idx_scan) OVER (ORDER BY relid,snap_date) ,'0') as index_scans_per_sec,
    	coalesce( idx_tup_fetch - LAG(idx_tup_fetch, 1, idx_tup_fetch) OVER (ORDER BY relid,snap_date) ,'0') / {0} as index_rows_fetched_per_sec,
    	coalesce( n_tup_ins - LAG(n_tup_ins, 1, n_tup_ins) OVER (ORDER BY relid,snap_date) ,'0') / {0} as rows_inserted_per_sec,
    	coalesce( n_tup_upd - LAG(n_tup_upd, 1, n_tup_upd) OVER (ORDER BY relid,snap_date) ,'0') / {0} as rows_updated_per_sec,
    	coalesce( n_tup_del - LAG(n_tup_del, 1, n_tup_del) OVER (ORDER BY relid,snap_date) ,'0') / {0} as rows_deleted_per_sec,
    	coalesce( n_tup_hot_upd - LAG(n_tup_hot_upd, 1, n_tup_hot_upd) OVER (ORDER BY relid,snap_date) ,'0') / {0} as rows_hot_updated_per_sec,
    	coalesce( n_live_tup - LAG(n_live_tup, 1, n_live_tup) OVER (ORDER BY relid,snap_date) ,'0') / {0} as live_row_count_per_sec,
    	coalesce( n_dead_tup - LAG(n_dead_tup, 1, n_dead_tup) OVER (ORDER BY relid,snap_date) ,'0') / {0} as n_dead_tup_per_sec
        from statz.table_activity ) as foo where foo.interval >'0s';
    """.format(finterval)
    cursor = conn.cursor()
    cursor.execute(table_statz_sql)

def run():
    conn_init()
    schema_init()
    start_time = time.time()
    while (time.time() - start_time) < fduration:
        main_task()
        time.sleep(finterval)
        db_statz()
        table_statz()
if __name__ == "__main__":
    run()
