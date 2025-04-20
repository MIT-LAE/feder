from threading import Thread, Event

from feder.server.rmq import RPCMessage

from .messages import (  # noqa
    FibonacciRequest, FibonacciResponse,
    FactorialRequest, FactorialResponse
)


def fib(n):
    if n < 2:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


def fact(n):
    if n < 2:
        return 1
    else:
        return n * fact(n - 1)


def fib_proc(rmq, msg):
    response = FibonacciResponse(
        name=msg.message.name,
        data=fib(msg.message.data),
        success=True
    )
    rmq.rpc_reply(msg, response)


def fact_proc(rmq, msg):
    if msg.message.data == 3:
        # Simulate breakagee on the server side.
        return
    response = FactorialResponse(
        name=msg.message.name,
        data=fact(msg.message.data),
        success=True
    )
    rmq.rpc_reply(msg, response)


def rpc_server(rmq):
    threads = []

    stopping = False
    while not stopping:
        msg = rmq.out_queue.get()
        # print(f'SERVER: {msg}')
        match msg:
            case 'STOP':
                stopping = True
            case RPCMessage():
                # print(f'RPC: {msg.endpoint} => {msg.message.name}')
                match msg.endpoint:
                    case 'fibonacci':
                        t = Thread(target=fib_proc, args=(rmq, msg,))
                        threads.append(t)
                        t.start()
                    case 'factorial':
                        t = Thread(target=fact_proc, args=(rmq, msg,))
                        threads.append(t)
                        t.start()
                    case other:
                        print(f'UKNOWN REQUEST TYPE: {other}')

    for t in threads:
        t.join()


def test_rpc(rmq_rpc_client, rmq_rpc_server):
    callback_complete = Event()
    callbacks_ok = {}
    callbacks_errored = set()
    saved_correlation_ids = {}

    def result_callback(correlation_id, response):
        if correlation_id not in saved_correlation_ids:
            return

        name, result = saved_correlation_ids[correlation_id]
        # print(f'{correlation_id} ({type(response)})  Name: {response.name} ?= {name}  Result: {response.data} ?= {result}  Success: {response.success}')
        callbacks_ok[name] = (
            response.name == name and
            response.data == result and
            response.success
        )
        del saved_correlation_ids[correlation_id]
        if len(saved_correlation_ids) == 1 and len(callbacks_errored) == 1:
            callback_complete.set()

    def error_callback(correlation_id, reason):
        # print(f'RPC ERROR: {correlation_id} => {reason}')
        callbacks_errored.add(correlation_id)
        if len(saved_correlation_ids) == 1 and len(callbacks_errored) == 1:
            callback_complete.set()

    def client():
        while True:
            qmsg = rmq_rpc_client.out_queue.get()
            if qmsg == 'STOP':
                return

    server_thread = Thread(target=rpc_server, args=(rmq_rpc_server,))
    server_thread.start()
    client_thread = Thread(target=client)
    client_thread.start()

    # Make some RPC calls.
    for n in range(1, 10):
        if n % 2 == 0:
            request_class = FibonacciRequest
            endpoint = 'fibonacci'
            fn = fib
        else:
            request_class = FactorialRequest
            endpoint = 'factorial'
            fn = fact
        name = f'{endpoint}-{n}'
        request = request_class(name=name, data=n)
        # print(f'Sending {endpoint} request: {name} {n}')
        correlation_id = rmq_rpc_client.send_rpc(
            endpoint, request, result_callback, error_callback, timeout=1
        )
        saved_correlation_ids[correlation_id] = (name, fn(n))

    callback_complete.wait(timeout=3)
    if not callback_complete.is_set():
        print('TIMED OUT!')
    rmq_rpc_server.out_queue.put('STOP')
    rmq_rpc_client.out_queue.put('STOP')
    server_thread.join()
    client_thread.join()

    assert len(callbacks_ok) == 8 and len(callbacks_errored) == 1 and all(callbacks_ok.values())
