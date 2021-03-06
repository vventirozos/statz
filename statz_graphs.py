#!/usr/local/bin/python
import sys
import psycopg2
import argparse
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as tick
import seaborn as sns
sns.set_style("whitegrid")

parser = argparse.ArgumentParser(description="Description goes here")
parser.add_argument('-c','--connection', default="dbname=postgres", help="""Connection string for use by psycopg. Defaults to "dbname=postgres" (local socket connecting to postgres database). dbname parameter in connection string is required.""")
parser.add_argument('-t', dest='table_to_graph', nargs='*', help="Table you want to get graphs for.")
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
    conn = psycopg2.connect(args.connection)
    return conn

def get_tablestatz(table_name):
    conn = conn_init()
    cur = conn.cursor()
    query = """SELECT snap_date,
                    step,
                    table_name,
                    seq_scans_per_sec,
                    seq_rows_read_per_sec,
                    index_scans_per_sec,
                    index_rows_fetched_per_sec,
                    rows_inserted_per_sec,
                    rows_updated_per_sec,
                    rows_deleted_per_sec,
                    rows_hot_updated_per_sec
                from statz.table_stats_per_sec
                where table_name like '%{0}'; """.format(table_name)
    cur.execute(query)
    tabledata = cur.fetchall()
    conn.close()
    return tabledata

def get_dbstatz():
    conn = conn_init()
    cur = conn.cursor()
    query = """SELECT snap_date,
                commits_per_sec,
                rollbacks_per_sec,
                rows_returned_per_sec,
                rows_fetched_per_sec,
                rows_inserted_per_sec,
                rows_updated_per_sec,
                rows_deleted_per_sec,
                blocks_read_per_sec,
                blocks_hit_cached_per_sec,
                txn_per_sec,
                cache_hit_ratio
                FROM statz.db_stats_per_sec;"""
    cur.execute(query)
    dbdata = cur.fetchall()
    conn.close()
    return dbdata

def get_sysstatz():
    conn = conn_init()
    cur = conn.cursor()
    sys_query = """
                select
                snap_date,
                cpu_load,
                interrupts,
                (read_bytes / 1024)/1024,
                (write_bytes / 1024)/1024,
                (used / 1024)/1024 as ram_used_mb,
                (free / 1024)/1024 as ram_free_mb
                from statz.system_activity
                order by 1
    """
    cur.execute(sys_query)
    sys_data = cur.fetchall()
    conn.close()
    return sys_data

def plot_sysstatz():
    sys_data = get_sysstatz()
    values = zip(*sys_data)

    time = values[0]
    cpu_load = values[1]
    interrupts = values[2]
    read_Mbytes_per_sec = values[3]
    write_Mbytes_per_sec = values[4]
    ram_used_mb = values[5]
    ram_free_mb = values[6]


    plt.suptitle('System statistics', fontsize=28, fontweight='bold')
    plt.subplot(3, 2, 2)
    plt.plot(time,interrupts, label='Interrupts' )
    plt.title('Interrupts')
    plt.xticks(rotation=45)
    plt.subplot(3, 2, 1)
    plt.plot(time,cpu_load, label='CPU load')
    plt.title('CPU load')
    plt.xticks(rotation=45)
    plt.subplot(3, 2, 5)
    plt.plot(time,ram_used_mb, label='RAM used (mb)')
    plt.title('RAM used (mb)')
    plt.xticks(rotation=45)
    plt.subplot(3, 2, 6)
    plt.plot(time,ram_free_mb, label='RAM free (mb)')
    plt.title('RAM free (mb)')
    plt.xticks(rotation=45)
    plt.subplot(3, 2, 3)
    plt.plot(time,read_Mbytes_per_sec, label='IO read (mb)')
    plt.title('IO read (mb)')
    plt.xticks(rotation=45)
    plt.subplot(3, 2, 4)
    plt.plot(time,write_Mbytes_per_sec, label='IO write (mb)')
    plt.title('IO write (mb)')
    plt.xticks(rotation=45)

    plt.subplots_adjust(top=0.90, bottom=0.08, left=0.05, right=0.95, hspace=0.45,wspace=0.15) #####
    figure = plt.gcf() # get current figure
    figure.set_size_inches(30, 15)
    plt.savefig("sys_plot.png", dpi = 100)
    #plt.show()
    plt.gcf().clear()



def plot_dbstatz():
    dbdata = get_dbstatz()
    values = zip(*dbdata)
#
    time = values[0]
    commits_per_sec = values[1]
    rollbacks_per_sec = values[2]
    rows_returned_per_sec = values[3]
    rows_fetched_per_sec = values[4]
    rows_inserted_per_sec = values[5]
    rows_updated_per_sec = values[6]
    rows_deleted_per_sec = values[7]
    blocks_read_per_sec = values[8]
    blocks_hit_cached_per_sec = values[9]
    txn_per_sec = values[10]
    cache_hit_ratio = values[11]
#
    plt.suptitle('Database statistics / sec', fontsize=28, fontweight='bold')
    plt.subplot(3, 4, 1)
    plt.plot(time,commits_per_sec, label='Commits / sec' )
    plt.title('Commits / sec')
    plt.xticks(rotation=45)
    plt.subplot(3, 4, 2)
    plt.plot(time,rollbacks_per_sec, label='Rollbacks / sec' )
    plt.title('Rollbacks / sec')
    plt.xticks(rotation=45)
    plt.subplot(3, 4, 3)
    plt.plot(time,rows_returned_per_sec, label='Rows returned / sec' )
    plt.title('Rows returned / sec')
    plt.xticks(rotation=45)
    plt.subplot(3, 4, 4)
    plt.plot(time,rows_fetched_per_sec, label='Rows fetched / sec')
    plt.title('Rows fetched / sec')
    plt.xticks(rotation=45)
    plt.subplot(3, 4, 5)
    plt.plot(time,rows_inserted_per_sec, label='Rows inserted / sec')
    plt.title('Rows Inserted / sec')
    plt.xticks(rotation=45)
    plt.subplot(3, 4, 6)
    plt.plot(time,rows_updated_per_sec, label='Rows updated / sec')
    plt.title('Rows updated / sec')
    plt.xticks(rotation=45)
    plt.subplot(3, 4, 7)
    plt.plot(time,rows_deleted_per_sec, label='Rows deleted / sec')
    plt.title('Rows deleted / sec')
    plt.xticks(rotation=45)
    plt.subplot(3, 4, 8)
    plt.plot(time,blocks_read_per_sec, label='Blocks read / sec')
    plt.title('Blocks Inserted / sec')
    plt.xticks(rotation=45)
    plt.subplot(3, 4, 9)
    plt.plot(time,blocks_hit_cached_per_sec, label='Blocks hit cached / sec')
    plt.title('Blocks hit cached / sec')
    plt.xticks(rotation=45)
    plt.subplot(3, 4, 11)
    plt.plot(time,txn_per_sec, label='Transactions / sec')
    plt.title('transactions / sec')
    plt.xticks(rotation=45)
    plt.subplot(3, 4, 12)
    plt.plot(time,cache_hit_ratio, label='Cache hit ratio')
    plt.title('Cache hit ratio')
    plt.xticks(rotation=45)

    plt.subplots_adjust(top=0.90, bottom=0.08, left=0.05, right=0.95, hspace=0.45,wspace=0.15) #####
    figure = plt.gcf() # get current figure
    figure.set_size_inches(30, 15)
    plt.savefig("db_plot.png", dpi = 100)
    #plt.show()
    plt.gcf().clear()

def plot_tablestatz(table_name):
    tabledata = get_tablestatz(table_name)
    values = zip(*tabledata)
#
    time = values[0]
    seq_scans_per_sec = values[3]
    seq_rows_read_per_sec = values[4]
    index_scans_per_sec = values[5]
    index_rows_fetched_per_sec = values[6]
    rows_inserted_per_sec = values[7]
    rows_updated_per_sec = values[8]
    rows_deleted_per_sec = values[9]
    rows_hot_updated_per_sec = values[10]
#
    tabletitle = """{0}""".format(table_name)
    plt.suptitle('Table statistics / sec for ' + tabletitle, fontsize=28, fontweight='bold')
    plt.subplot(2, 4, 1)
    plt.plot(time,seq_scans_per_sec, label='Sequential scans / sec' )
    plt.title('Sequential scans / sec')
    plt.xticks(rotation=45)
    plt.subplot(2, 4, 2)
    plt.plot(time,seq_rows_read_per_sec )
    plt.title('Sequential rows read / sec')
    plt.xticks(rotation=45)
    plt.subplot(2, 4, 3)
    plt.plot(time,index_scans_per_sec )
    plt.title('Index scans / sec')
    plt.xticks(rotation=45)
    plt.subplot(2, 4, 4)
    plt.plot(time,index_rows_fetched_per_sec, label='Rows fetched from index / sec')
    plt.title('Rows fetched from index / sec')
    plt.xticks(rotation=45)
    plt.subplot(2, 4, 5)
    plt.plot(time,rows_inserted_per_sec, label='Rows inserted / sec')
    plt.title('Rows inserted / sec')
    plt.xticks(rotation=45)
    plt.subplot(2, 4, 6)
    plt.plot(time,rows_updated_per_sec, label='Rows updated / sec')
    plt.title('Rows updated / sec')
    plt.xticks(rotation=45)
    plt.subplot(2, 4, 7)
    plt.plot(time,rows_deleted_per_sec, label='Rows deleted / sec')
    plt.title('Rows deleted / sec')
    plt.xticks(rotation=45)
    plt.subplot(2, 4, 8)
    plt.plot(time,rows_hot_updated_per_sec, label='Rows hot updated / sec')
    plt.title('Rows hot updated / sec')
    plt.xticks(rotation=45)

    plt.subplots_adjust(top=0.90, bottom=0.10, left=0.05, right=0.95, hspace=0.30,wspace=0.15) #####
    figure = plt.gcf() # get current figure
    figure.set_size_inches(30, 15)

    tablefile = tabletitle +"_plot" + ".png"
    #print tablefile
    plt.savefig(tablefile, dpi = 100)
    plt.gcf().clear()

def main():
    tablez = []
    tablez = args.table_to_graph
    plot_dbstatz()
    plot_sysstatz()
    if tablez:
        for table in tablez:
            plot_tablestatz(table)
    else:
        exit(0)

if __name__ == "__main__":
    main()
