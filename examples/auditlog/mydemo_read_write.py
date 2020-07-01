from ethvigil.EVCore import EVCore
import threading
import random
from functools import wraps
import queue
import asyncio
import time
import signal
import json
from websocket_listener import consumer_contract, EthVigilWSSubscriber
from exceptions import ServiceExit

update_q = queue.Queue()

evc = EVCore(verbose=False)
api_read_key = evc._api_read_key

t = EthVigilWSSubscriber(kwargs={
        'api_read_key': api_read_key,
        'update_q': update_q,
        'ev_loop': asyncio.get_event_loop()
    })
t.start()

def handle_service_exit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (ServiceExit, KeyboardInterrupt):
            t.shutdown_flag.set()
            print('Waiting for Websocket subscriber thread to join (Ctrl-C in case thread is stuck)...')
            t.join()
            print('Websocket subscriber thread exited')
    return wrapper

async def async_shutdown(signal, loop):
    # print(f'Received exit signal {signal.name}...')
    # print('Nacking outstanding messages')
    tasks = [t for t in asyncio.Task.all_tasks() if t is not
             asyncio.Task.current_task()]

    [task.cancel() for task in tasks]

    # print(f'Cancelling {len(tasks)} outstanding tasks')
    await asyncio.gather(*tasks)
    loop.stop()
    # print('Event loop Shutdown complete.')


def sync_shutdown(signum, frame):
    # print('Caught signal %d' % signum)
    raise ServiceExit


def deploy_contracts():
    print('Deploying myDemoContract...')
    deploy_demo_contract_response = evc.deploy(
        contract_file='myDemoContract.sol',
        contract_name='myDemoContract',
        inputs=dict(
            initLicenseid='RANDOMLICENSEID',
            initNote='RANDOMNOTE'
        )
    )
    demo_contract_deploying_tx = deploy_demo_contract_response['txhash']
    demo_contract_addr = deploy_demo_contract_response['contract']
    print('Deploying myAuditLog...')
    deploy_audit_log_contract_response = evc.deploy(
        contract_file='myAuditLog.sol',
        contract_name='myAuditLog',
        inputs=dict()
    )
    audit_log_deploying_tx = deploy_audit_log_contract_response['txhash']
    audit_log_contract_addr = deploy_audit_log_contract_response['contract']
    demo_contract_deployed = False
    audit_contract_deployed = False
    print('Waiting for deployment confirmations...')
    while True:
        websocket_payload = update_q.get()
        websocket_payload = json.loads(websocket_payload)
        if websocket_payload.get('type') == 'contractmon' or websocket_payload.get('type') == 'otm':
            if websocket_payload['txHash'] == demo_contract_deploying_tx:
                print('\nReceived myDemoContract deployment confirmation: ', demo_contract_deploying_tx)
                demo_contract_deployed = True
            elif websocket_payload['txHash'] == audit_log_deploying_tx:
                print('\nReceived myAuditLog deployment confirmation: ', audit_log_deploying_tx)
                audit_contract_deployed = True
        update_q.task_done()
        if demo_contract_deployed and audit_contract_deployed:
            break
        time.sleep(1)
    return demo_contract_addr, audit_log_contract_addr

@handle_service_exit
def main():
    demo_contract, auditlog_contract = deploy_contracts()
    demo_contract_instance = evc.generate_contract_sdk(
        contract_address=demo_contract,
        app_name='myDemoContract'
    )
    auditlog_contract_instance = evc.generate_contract_sdk(
        contract_address=auditlog_contract,
        app_name='myAuditLog'
    )

    last_tx = None
    while True:
        if not last_tx:
            params = {'incrValue': random.choice(range(1, 255)), '_note': 'NewNote' + str(int(time.time())) }
            last_tx = demo_contract_instance.setContractInformation(**params)[0]['txHash']
            print('\n\nSending tx to setContractInformation with params: ', params)
            print('setContractInformation tx response: ', last_tx)
            print('Waiting for event update payload...')
        p = update_q.get()
        p = json.loads(p)

        if p.get('type') == 'event' \
                and p['event_name'] == 'ContractIncremented' \
                and p['txHash'] == last_tx:
            print('\nReceived websocket payload:', p, '\n\n')
            print('Received setContractInformation event confirmation: ', last_tx)
            last_tx = None
            print('Writing to audit log contract...')
            audit_tx = auditlog_contract_instance.addAuditLog(
                _newNote=p['event_data']['newNote'],
                _changedBy=p['event_data']['incrementedBy'],
                _incrementValue=p['event_data']['incrementedValue'],
                _timestamp=p['ctime']
            )
            print('Wrote to audit log contract. Tx response: ', audit_tx[0]['txHash'])
            # quit after the first transaction, feel free to comment this out to try multiple calls
            raise KeyboardInterrupt
        update_q.task_done()
        time.sleep(5)


if __name__ == '__main__':
    main_loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        main_loop.add_signal_handler(
            s, lambda s=s: asyncio.get_event_loop().create_task(async_shutdown(s, main_loop)))
    for s in signals:
        signal.signal(s, sync_shutdown)
    try:
        main()
    except ServiceExit:
        pass
