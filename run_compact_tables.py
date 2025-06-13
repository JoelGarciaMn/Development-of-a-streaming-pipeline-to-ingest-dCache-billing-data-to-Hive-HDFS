import os

import datetime

import argparse

import Dcache_kafka_to_hive as dkth

def def_arguments():

    parser = argparse.ArgumentParser()

    parser.add_argument('--database', '-d', help = 'database on which the tables are placed')
    parser.add_argument('--transfer', '-tr', help = 'name of the transfer table')
    parser.add_argument('--request', '-rq', help = 'name of the request table')
    parser.add_argument('--storage', '-st', help = 'name of the storage table')
    parser.add_argument('--remove', '-rm', help = 'name of the remove table')
    

    parser.add_argument('--bootstrap_servers', '-b', help = 'code of the kafka bootstrap servers')
    parser.add_argument('--pattern', '-p', help = 'pattern of kafka from which the data is recieved')

    parser.add_argument('--partition', '-pt', default='yesterday', help = """partition to compact, options: -yesterday: it compacts the partition from yeseterday.
    -all: it compacts all the partitions.
    -YYYY-MM-DD,YYYY-MM-DD,...: it compacts the partitions of the dates specified (the list mustn't have spaces after the comas)""")
    
    parser.add_argument('--loglevel', '-l', default='INFO')

    parser.add_argument('--lock_path', '-lp', help = 'path were the lock and state file will be stored')

    return parser


def main(args):
    """
    The function defines the partitions that it has to compact, once that is done, if there is a .lock file the 
    function stops and prints 1 to the status file. If not, it tries to do the compaction and creates a .lock file,
    if there is no error on the compaction process it prints 0 on thestatus file, if the compaction fails 1 is printed 
    into the status file. Once it ends it eliminates the .lock file
    """
    lock_file_path = os.path.join(args.lock_path + "/", "run_stream.lock")

    status_file_path = os.path.join(args.lock_path + "/", "status_run_compact.txt")

    test_file_path = os.path.join(args.lock_path + "/", "test.txt")
    
    if args.partition == "yesterday":
        today = datetime.date.today()
        partition = [str(today - datetime.timedelta(days = 1))]

    elif args.partition =="all":
        partition = None
        
    else:
        intermediate = args.partition
        print(intermediate)
        partition = intermediate.split(",")
        print(partition)
        
    try:

        if not os.path.exists(lock_file_path):
            
            lock_file = open(lock_file_path, "w")
            lock_file.close()

            spark = dkth.get_spark_session(loglevel=args.loglevel)
        
            streamer = dkth.Streaming(
                args.database, args.transfer, args.request,
                args.storage, args.remove, args.bootstrap_servers,
                args.pattern, spark
            )


            
            streamer.repartition((partition))

            os.remove(lock_file_path)
            
            status = 0
           
        else:
            status = 1
            print("The streaming is currently running in another instance")
    except:
        status = 1
        os.remove(lock_file_path)
    finally:
        status_file = open(status_file_path, "w")
        status_file.write(str(status))
        status_file.close()



if __name__ == '__main__':

    parser = def_arguments()
    args = parser.parse_args()
    main(args)
