import argparse

import os

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
    
    parser.add_argument('--trigger_s', '-ts', type = int, help = 'interval of time between two batches of the streaming')
    parser.add_argument('--checkpoint', '-c', help = 'folder were the checkpoint files of the streaming will be stored')

    parser.add_argument('--loglevel', '-l', default='INFO')
    
    parser.add_argument('--lock_path', '-lp', help = 'path were the lock and state file will be stored')

    return parser


def main(args):
    """
    If there is a .lock file, prints 1 on the status file. If there is not a .lock file the streaming is started, if it ends without an error 
    it prints 0 on the status file and eliminates the .lock file, if there is an error it prints a 1 in the status file and eliminates the
    .lock file.
    """
    lock_file_path = os.path.join(args.lock_path + "/", "run_stream.lock")
    
    status_file_path = os.path.join(args.lock_path +"/", "status_run_stream.txt")
    try:
        if not os.path.exists(lock_file_path):
            lock_file = open(lock_file_path, "w")
            lock_file.close()
            
            
            spark = dkth.get_spark_session(loglevel=args.loglevel)
            
            tables = dkth.Tables(args.database, spark)
            tables.create_transfer(args.transfer)
            tables.create_request(args.request)
            tables.create_storage(args.storage)
            tables.create_remove(args.remove)
            streamer = dkth.Streaming(
                args.database, args.transfer, args.request,
                args.storage, args.remove, args.bootstrap_servers,
                args.pattern, spark
            )
            streamer.to_hive(args.trigger_s, args.checkpoint)
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
