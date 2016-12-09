import socket
import sys
import getopt
import select
from multiprocessing import Pool
import logging
import time

# Establishes socket
# Sends grep command and returns result
def send_query(query, conn):
    command = query
    host = conn['host']
    ret = ''
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((conn['host'], int(conn['port'])))
        send_msg = (command)
        sock.send(send_msg)

        data = ''
        try:
            while True:
                buf = sock.recv(8192)
                data = data + buf
                if(data.find('CMD_END') != -1):
                    break
            if(data.find('CMD_FAIL_FROM_REMOTE') == -1):
                ret = data.strip('CMD_END')

        except socket.timeout:
            pass
        sock.close()
    except (socket.error,socket.gaierror) as err_msg:
        pass

    return ret

# grepper class that handles both query and connections
class grepper:

    def __init__(self, query):
        self.query = query

    def __call__(self, conn):
        return send_query(self.query, conn)


# Create a dictionary of connection with  host, port
def read_servers(filename):
    connections = []
    with open(filename, 'r') as my_file:
        for line in my_file:
            line = line.strip('\n')
            # conn = line.split(',')
            host = line
            port = '10005'
            dict = {"host": host, "port": port}
            connections.append(dict)
        logging.info("List of Connections\n")
        logging.info(connections)
        return connections

# Output results from each connection
def output_results(conns, results):
    dist_lcount = 0
    for i in range(len(conns)):
        lines = results[i].splitlines()
        for line in lines:
            print '{0}: {1}'.format(conns[i]['host'], line)
        lcount = len(lines)
        dist_lcount += lcount
        print 'line count [{0}]: {1}'.format(conns[i]['host'], lcount)
    print 'Distributed line count: {0}'.format(dist_lcount)

# Counts the total amount of lines
def get_tcnt(results):
    tcnt = 0 
    for result in results:
        lines = result.splitlines()
        tcnt = tcnt + len(lines)
    return tcnt
        
# Distributed the grep command to multiple servers
# Relies on pooling
def dgrep(query, conns):
    tank = Pool(len(conns))
    results = tank.map(grepper(query), conns)
    tank.close()
    tank.join
    return results

# Generates a list of dictionaries with hostname
# Multiprocess query grep function using Pool object
def main_sub(filename, query):

    conns = read_servers(filename)
    t0 = time.clock()
    w0 = time.time()
    results = dgrep(query, conns)
    t1 = time.clock() - t0
    w1 = time.time() - w0
    t2 = time.clock()
    w2 = time.time()
    output_results(conns, results)
    t3 = time.clock() - t2
    w3 = time.time() - w2
    # print("Query Clock Time = ", t1)
    # print("Query Wall Time = ", w1)
    # print("Display Clock Time = ", t3)
    # print("Dsplay Wall Time = ", w3)
    return get_tcnt(results) 

# Receives command line arguments and calls main function
def main():
    if len(sys.argv) < 3:
        print 'python client.py <filename> <grep arguments>'
        sys.exit(1)
    logging.info("Cmd line arguments\n")
    logging.info(sys.argv[2:])
    command = ' '.join(sys.argv[2:])
    logging.info("Grep command :" + command)
    file_name = sys.argv[1]
    main_sub(file_name, command)



if __name__ == "__main__":
    logging.basicConfig(filename = "clientdebug.log", level = logging.INFO,filemode = "w")
    main()
