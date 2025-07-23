
import asyncio
import hashlib
import json
import logging
import shutil
import signal
import sys
import time
import subprocess as SP
from tempfile import TemporaryDirectory
from pathlib import Path

from p4p import Value
from p4p.nt import NTScalar, NTEnum
from p4p.server import Server, ServerOperation
from p4p.server.asyncio import SharedPV

_log = logging.getLogger(__name__)

def getargs():
    from argparse import ArgumentParser
    P = ArgumentParser()
    P.add_argument('-v', '--verbose',
                   dest='level', default=logging.INFO,
                   action='store_const', const=logging.DEBUG,
                   help='Make more noise')
    P.add_argument('--prefix', default='FDAS:',
                   help='Record prefix')
    P.add_argument('--store', type=Path, default=Path('/data/cccr'),
                   help='Path prefix for archival storage of CCCR')
    P.add_argument('--sim', dest='doit', default=True, action='store_false',
                   help='Do not actually invoke configurer')
    return P

async def amain(args):
    _log.debug('Starting')
    loop = asyncio.get_running_loop()

    # our PVs
    pv_fname = SharedPV(nt=NTScalar('s'))
    pv_fname.open('', severity=3)
    pv_content = SharedPV(nt=NTScalar('s'))
    pv_content.open('', severity=3)
    pv_hash = SharedPV(nt=NTScalar('s'))
    pv_hash.open('', severity=3)
    #pv_output = SharedPV(nt=NTScalar('s'), initial='')
    pv_message = SharedPV(nt=NTScalar('s'), initial='Startup')
    pv_log = SharedPV(nt=NTScalar('s'), initial='Startup')
    pv_status = SharedPV(nt=NTEnum(), initial={
        'choices':['Error', 'Success'],
        'index': 1,
    })
    pv_busy = SharedPV(nt=NTEnum(), initial={
        'choices':['Idle', 'Busy'],
        'index': 0,
    })

    # handle setting of new CCCR filename.
    # expected to happen before each write to CCCR body.
    fname: Path = None
    fname_who: str = None
    @pv_fname.put
    def onPut(pv:SharedPV, op:ServerOperation):
        nonlocal fname, fname_who
        val:Value = op.value().raw
        if val.changed('value'):
            T = val.value
            if len(T)<5:
                raise ValueError('CCCR name too short')
            fname = T
            fname_who = op.account()
            pv.post(T, timestamp=time.time(), severity=0)
            _log.debug('Set CCCR filename %r by %r', fname, fname_who)
        op.done()

    # handle setting of CCCR content
    busy = False
    @pv_content.put
    async def onPut(pv:SharedPV, op:ServerOperation):
        nonlocal fname, fname_who, busy
        now = time.time()
        who = op.account()
        # TODO: check op.roles()
        try:
            if busy:
                raise RuntimeError('Concurrent load not permitted')
            busy = True
            pv_busy.post(1, timestamp=now)

            if fname is None:
                raise ValueError('Must set CCCR name before content')

            if who!=fname_who: # TODO: weak auth...
                raise RuntimeError(f'Collidated transaction {fname_who} | {who}')

            cont = op.value()
            if len(cont)<5:
                raise ValueError('CCCR content too short')

            cont = cont.encode()
            h256 = hashlib.sha256(cont).hexdigest()

            # archival location
            # /data/cccr/5a/5a235f/5a235f---
            farch = args.store / h256[:2] / h256[:6] / h256
            farch.parent.mkdir(parents=True, exist_ok=True)

            try:
                with farch.open('xb') as F:
                    F.write(cont)
            except FileExistsError:
                if farch.read_bytes()!=cont:
                    # maybe just a trucated write, but can't distinguish...
                    _log.error('Wow!  Alert the press.  A real world sha256 collision %s', h256)
                    raise RuntimeError('sha256 hash collision!!!!')
                else:
                    _log.debug('Found input CSV in archive')
                # probably re-load of previous configuration

            with TemporaryDirectory() as tdir:
                tdir = Path(tdir)

                # TODO: pass in requesting user?
                cmd = [
                    sys.executable, '-m', 'cccr_configurer.configurer',
                    '--input', str(farch),
                    '--output', str(tdir), # will write output.csv
                    '--prefix', args.prefix,
                ]
                if not args.doit:
                    cmd.append('--sim')
                _log.debug('Run: %r', cmd)

                pv_log.post('', timestamp=now-2e-9)

                capture = tdir / 'log.txt'
                with capture.open('w+') as OUT:
                    P=await asyncio.create_subprocess_exec(*cmd,
                                                            cwd=Path(__file__).parent.parent,
                                                            stdout=OUT,
                                                            stderr=SP.STDOUT)
                    try:
                        async with asyncio.timeout(30): # configurer.py has a much shorter internal timeout.  So this is paranoia...
                            await P.wait()
                    except asyncio.TimeoutError:
                        _log.error('Timeout running: %r', cmd)
                        P.terminate()
                        raise
                    else:
                        OUT.seek(0)
                        Log = OUT.read()
                        pv_log.post(Log, timestamp=now)
                        for L in Log.splitlines():
                            _log.error(L.strip())
                        if P.returncode!=0:
                            raise RuntimeError(f'configurer error {P.returncode}')

                # TODO: do something with output.csv

            pv_content.post(cont, timestamp=now, severity=0)
            pv_hash.post(h256, timestamp=now, severity=0)
            pv_message.post('Success', timestamp=now, severity=0)
            pv_status.post(1, timestamp=now, severity=0)
            op.done() # notify of success
        except asyncio.CancelledError as e:
            op.done(error=str(e))
            pv_message.post('Cancel', timestamp=now, severity=2)
            pv_status.post(0, timestamp=now, severity=2)
            raise
        except Exception as e:
            _log.exception('oops')
            op.done(error=str(e))
            pv_message.post(str(e), timestamp=now, severity=2)
            pv_status.post(0, timestamp=now, severity=2)
        finally:
            busy, fname, fname_who = False, None, None
            pv_busy.post(0, timestamp=now)

    # run until interrupted
    with Server([{
            f'{args.prefix}CCCR:NAME': pv_fname,
            f'{args.prefix}CCCR:BODY': pv_content,
            f'{args.prefix}CCCR:HASH': pv_hash,
            #f'{args.prefix}CCCR:OUT': pv_output,
            f'{args.prefix}CCCR:MSG': pv_message,
            f'{args.prefix}CCCR:STS': pv_status,
            f'{args.prefix}CCCR:LOG': pv_log,
            f'{args.prefix}CCCR:BUSY': pv_busy,
        }]):
        done = asyncio.Event()
        loop.add_signal_handler(signal.SIGINT, done.set)
        loop.add_signal_handler(signal.SIGTERM, done.set)
        _log.info('Running')
        await done.wait()
        _log.debug('Stopping')

    _log.info('Done')

def main():
    args = getargs().parse_args()
    logging.basicConfig(level=args.level,
                        format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
    asyncio.run(amain(args))

if __name__=='__main__':
    main()
