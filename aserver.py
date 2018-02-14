'''
this is a simple example of asyncio socket programming to read concurrent
TCP connections and put the data to a queue to print it out by another
function on the console.
'''

from socket import *
import asyncio
import logging
import argparse

sock = None

async def server(queue, args, loop):
    '''
    server listens to new connection and pass the client to
    handle_client function to get data and put it to queue
    '''

    global sock
    family = AF_INET

    if args.ipv6:
        family = AF_INET6

    try:
        address = (args.address, int(args.port))

        sock = socket(family=family,type=SOCK_STREAM,proto=0)
        sock.settimeout(1.0)
        sock.bind(address)
        sock.listen(1)
        sock.setblocking(False)

        logging.info('server has been started ...')

        while True:
            client, addr = await loop.sock_accept(sock)
            logging.debug(f'{addr} connected!')
            loop.create_task(handle_client(client, queue, loop))

    except Exception as e:
        logging.error(str(e))

async def handle_client(client, queue, loop):
    '''
    handle_client read the client and pass the data to queue
    '''

    while True:
        data = await loop.sock_recv(client, 1024)
        if not data:
            logging.info('connection closed')
            client.close()
            break
        await queue.put(data)

    client.close()


async def consumer(queue, loop):
    '''
    consumer gets the data and print it out on the console
    '''

    while True:
        item = await queue.get()
        if item is None:
            break
        logging.debug(f'{item}')        

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address', default='', help='--address address')
    parser.add_argument('-p', '--port', default=8080, help='--port port number')
    parser.add_argument('-6', '--ipv6', action="store_true", default=False, help='enable ipv6 binding')
    args = parser.parse_args()

    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    try:
        loop = asyncio.get_event_loop()
        queue = asyncio.Queue(loop=loop)
        loop.run_until_complete(asyncio.gather(server(queue, args, loop),
                                               consumer(queue, loop)))
    except KeyboardInterrupt as e:
        logging.debug('shutting down ...')
        sock.shutdown
        sock.close()
        loop.close()