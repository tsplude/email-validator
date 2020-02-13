import socket
import smtplib
import concurrent.futures
import pickle

import asyncio
import aiosmtplib

import pandas as pd
import dns.resolver


HOST = socket.gethostname()
TIMEOUT = 10


def get_addresses(n=None):
    df = pd.read_csv('emails.csv')
    df_emails = df[['SALESFORCE EMAILS']].rename({
        'SALESFORCE EMAILS': 'EMAIL'
    }, axis=1)
    return df_emails[:n] if n else df_emails


def _get_mxrec(address):
    domain = address.split('@')[1]
    records = dns.resolver.query(domain, 'MX')
    mxRec = records[0].exchange
    return str(mxRec)

async def _verify(email, collect_queue):
    passed = True
    try:
        mx_rec = _get_mxrec(email)
        async with aiosmtplib.SMTP(hostname=mx_rec, timeout=TIMEOUT) as serv:
            #await serv.connect(mx_rec)
            await serv.helo(HOST)
            await serv.mail('foo@bar.com')
            code, msg = await serv.rcpt(str(email))
            msg = "|".join(map(str, [email, code, msg]))
            if code == 550:
                passed = False
                print(msg)
    except (aiosmtplib.errors.SMTPRecipientRefused) as e: # aiosmtplib.errors.SMTPTimeoutError
        passed = False
    except Exception as e:
        print(f"ERROR|{email}|{e}|{type(e)}")
    if passed:
        await collect_queue.put(email)


async def _request_collector(collect_queue):
    good_emails = []
    while True:
        email = await collect_queue.get()
        if email is None:
            break
        good_emails.append(email)
    return good_emails

async def _request_consumer(request_queue, collect_queue):
    while True:
        email = await request_queue.get()
        if email is None:
            await request_queue.put(None)
            break
        await _verify(email, collect_queue)

async def _request_producer(df_address):
    request_queue = asyncio.Queue()
    collect_queue = asyncio.Queue()

    request_workers = [
        asyncio.ensure_future(_request_consumer(request_queue, collect_queue))
        for _ in range(20)
    ]

    collector = asyncio.ensure_future(_request_collector(collect_queue))

    for address in df_address['EMAIL']:
        await request_queue.put(address)

    await request_queue.put(None)
    await asyncio.wait(request_workers)

    await collect_queue.put(None)
    await asyncio.wait([collector])
    return collector.result()


def verify_fast(df_address):
    print(f"Verifying {len(df_address)} emails...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    producer = loop.create_task(_request_producer(df_address))
    loop.run_until_complete(producer)
    return producer.result()


def verify_slow(df_address):

    _HOST = socket.gethostname()
    _TIMEOUT = 10 # seconds

    serv = smtplib.SMTP()
    serv.set_debuglevel(0)

    bad_emails = []
    for a in df_address['EMAIL']:
        try:
            mx_rec = _get_mxrec(a)
            with smtplib.SMTP(host=mx_rec, local_hostname=_HOST, timeout=_TIMEOUT) as serv:
                serv.helo(_HOST)
                serv.mail('foo@bar.com')
                code, msg = serv.rcpt(str(a))
                msg = "|".join(map(str, [a, code, msg]))
                if code == 550:
                    print(msg)
                    bad_emails.append(a)
        except Exception as err:
            continue

    good_emails = df_address[~df_address['EMAIL'].isin(bad_emails)]
    return good_emails, bad_emails


def main():
    addresses = get_addresses()
    passed = verify_fast(addresses)

    with open('passed_emails.pkl','wb') as f:
        pickle.dump(passed, f)

    print(len(passed))


if __name__ == '__main__':
    main()


"""
def _verify_group(grp):

    _HOST = socket.gethostname()
    _TIMEOUT = 10 # seconds

    serv = smtplib.SMTP()
    serv.set_debuglevel(0)

    bad_emails = []
    for a in grp['EMAIL']:
        try:
            mx_rec = get_mxrec(a)

            with smtplib.SMTP(host=mx_rec, local_hostname=_HOST, timeout=_TIMEOUT) as serv:
                serv.helo(_HOST)
                serv.mail('foo@bar.com')
                code, msg = serv.rcpt(str(a))
                msg = "|".join(map(str, [a, code, msg]))
                if code == 550:
                    print(msg)
                    bad_emails.append(a)
        except Exception as err:
            continue
    return grp[~grp['EMAIL'].isin(bad_emails)]

def _verify(address_list):

    _HOST = socket.gethostname()
    _TIMEOUT = 5 # seconds

    for a in address_list:
        try:
            mx_rec = _get_mxrec(a)
            print(mx_rec, _HOST, _TIMEOUT)
            with smtplib.SMTP(timeout=_TIMEOUT) as serv:
                serv.set_debuglevel(1)
                serv.connect(mx_rec)
                serv.helo(_HOST)
                serv.mail('foo@bar.com')
                code, msg = serv.rcpt(str(a))
                print(a, code, msg)
        except Exception as err:
            print(a, "ERROR", err)
"""