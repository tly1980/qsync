#!/usr/bin/env python

import argparse
import logging
import re
import os
import multiprocessing
import functools

import boto

P = argparse.ArgumentParser()

FORMAT = '%(asctime)-15s %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)

def escape_s3uri(s3_uri):
    s3_uri = s3_uri + '/' if not s3_uri.endswith('/') else s3_uri
    bk_name, key_name = re.match('^s3://([^/]+)/(.*)$', s3_uri).groups()
    key_name = '' if key_name is None else key_name
    return bk_name, key_name

class Syncer(object):
    def __init__(self, src_base, dst_base, dry_run=False, logger_id=''):
        self.src_bk_name, self.src_key_base = escape_s3uri(src_base)
        self.dst_bk_name, self.dst_key_base = escape_s3uri(dst_base)
        self.dry_run = dry_run

        self.logger = logging.getLogger('Syncer-' + str(logger_id))

        conn = boto.connect_s3()

        self.dst_bk = conn.get_bucket(self.dst_bk_name)

    def dst_key(self, src_key):
        return self.dst_key_base + src_key[len(self.src_key_base):]

    def do(self, src_key):
        try:
            if not self.dry_run:
                self.dst_bk.copy_key(
                    self.dst_key(src_key), self.src_bk_name, src_key, preserve_acl=True)

                self.logger.info('completed, src: %s dst: %s' % (src_key, self.dst_key(src_key)))

            else:
                self.logger.info('dry-run, src: %s dst: %s' % (src_key, self.dst_key(src_key)))
        except Exception, e:
            self.logger.error(e)


THE_SYNCER = None

def run(src_base, dst_base, dry_run, src_key):
    #print src_base, dst_base, dry_run, src_key
    global THE_SYNCER
    if not THE_SYNCER:
        THE_SYNCER = Syncer(src_base, dst_base, dry_run=dry_run, logger_id=os.getpid())

    THE_SYNCER.do(src_key.name)

P.add_argument(
    'src',
    help='src s3 url',
    type=str
)    

P.add_argument(
    'dst',
    help='dst s3 url',
    type=str
)

P.add_argument(
    '--dry',
    default=False,
    action='store_true'
)

P.add_argument(
    '-c',
    '--concurrency',
    type=int,
    default=4,
    help='Size of process pool'
)


def main(args):
    src_bk, src_key_base = escape_s3uri(args.src)
    pool = multiprocessing.Pool(args.concurrency)
    the_run = functools.partial(run, args.src, args.dst, args.dry)
    conn = boto.connect_s3()
    src_bk = conn.get_bucket(src_bk)
    it = pool.imap(the_run, src_bk.list(prefix=src_key_base))
    for i in it:
        pass

if __name__== '__main__':
    main(P.parse_args())
