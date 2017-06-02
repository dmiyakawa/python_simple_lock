#!/usr/bin/env python3

"""\
Symbolic linkを介した簡易の排他ロック機構を提供する。

共有ロックの機能はない。
1つのプロセス内でスレッド等より複数使用することも想定されていない。

モジュールを直接実行するとLock自身のデモを実行できる。
最低2つのターミナルを準備し、readerとwriterとして動作させる。
それぞれで次のように実行する (meatmail-lite/ ディレクトリに移動すること)。

 $ python3 -m alowa_lite.lock reader
 $ python3 -m alowa_lite.lock writer

readerとwriterは所定のJSONファイルとロックファイルを用いて
「ロック→JSONファイルの読み書き」を繰り返す。
writerは適当にsleepをしつつ標準では10まで書き込む。
readerはJSONに指定された値(標準で10)が書き込まれるまで
ロックを取ってJSONを読むことを繰り返す。

正常であればエラーは発生せず、書込み状態に応じてreaderが
値を読み込んでいく。ロックが動作していなければ何らかの不審な挙動を
起こすはずである。
"""

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from logging import (
    getLogger, NullHandler, StreamHandler, Formatter, DEBUG, INFO
)
import errno
import json
import os
import random
import sys
import time

null_logger = getLogger('null')
null_logger.propagate = False
null_logger.addHandler(NullHandler())


class LockError(RuntimeError):
    pass


class SimpleLock():
    """\
    簡易の排他ロック機構を提供する。
    コンテクストマネージャであるため、with文にて利用可能。

    指定されたロックファイル用 path の他に path.(pid)
    という一時ファイルを各プロセス毎に作成する。
    ここでは前者をlockfile、後者をtmpfileと呼ぶ。
    それぞれのLockオブジェクトはtmpfileをまず個別に作成してlockfileへの
    ハードリンクを作成することを持ってロックとする。
    ハードリンクの作成に失敗したらタイムアウトするまで待機する。
    tmpfileにはtmpfile自身のパスが書かれている。
    これにより、ハードリンクが既に成立している際のlockfileの内容を
    読み取ることで自身がロックしたかどうかを確認出来る。

    単一プロセスで複数のロックを作成することは想定されていない。
    """
    def __init__(self, path, *, logger=None, no_sleep=False):
        self.logger = logger or null_logger
        self._lockfile_path = path
        self._tmpfile_path = '{}.{}'.format(path, os.getpid())
        self._timeout = 60  # sec
        self._no_sleep = False

    def __enter__(self):
        self.lock()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.unlock()

    def lock(self):
        logger = self.logger
        logger.debug('{}.lock()'.format(self.__class__.__name__))
        with open(self._tmpfile_path, 'w') as f:
            f.write(self._tmpfile_path)
        timeout = time.time() + self._timeout
        while True:
            try:
                os.link(self._tmpfile_path, self._lockfile_path)
                logger.debug('Obtained lock ({} -> {})'
                             .format(self._tmpfile_path, self._lockfile_path))
                break
            except OSError as e:
                if e.errno != errno.EEXIST:
                    logger.error('Unexpected OSError happend during lock ({})'
                                 .format(e))
                    raise
            if time.time() > timeout:
                raise LockError('Timeout')
            if not self._no_sleep:
                time.sleep(random.random() + 0.01)

    def unlock(self, force=False):
        logger = self.logger
        logger.debug('{}.unlock()'.format(self.__class__.__name__))
        if not self.is_locked():
            raise LockError('Not locked by myself')
        # force=True の場合、ロックに関するファイルがなくてもエラーとしない
        try:
            os.unlink(self._lockfile_path)
        except OSError as e:
            if not (force and e.errno == errno.ENOENT):
                raise
        try:
            os.unlink(self._tmpfile_path)
        except OSError as e:
            if not (force and e.errno == errno.ENOENT):
                raise

    def is_locked(self):
        return self._read_lockfile() == self._tmpfile_path

    def _read_lockfile(self):
        try:
            with open(self._lockfile_path) as f:
                content = f.read()
            return content
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
        return None


def _test_lock_writer(content_path, lock_path, n, logger):
    """\
    適当にsleepしながら1からnまでvalueをincrementする。
    """
    logger.info('Start running as a writer (content_path: {}, lock_path: {})'
                .format(content_path, lock_path))
    for i in range(1, n + 1):
        logger.debug('Trying acquiring lock')
        with SimpleLock(lock_path, logger=logger):
            with open(content_path, 'w') as f:
                time.sleep(random.random())
                logger.info('Writing {}'.format(i))
                f.write(json.dumps({'value': i}))
        time.sleep(random.random() + 0.2)
    logger.info('Finished running as a writer')


def _test_lock_reader(content_path, lock_path, n, logger):
    """\
    valueがnになるまで読取り続ける
    """
    logger.info('Start running as a reader (content_path: {}, lock_path: {})'
                .format(content_path, lock_path))
    previous_value = None
    while True:
        logger.debug('Trying acquiring lock')
        with SimpleLock(lock_path, logger=logger, no_sleep=True):
            try:
                with open(content_path) as f:
                    json_data = json.loads(f.read())
                    value = json_data['value']
                    if previous_value != value:
                        logger.info(value)
                        previous_value = value
                    if value == n:
                        break
            except FileNotFoundError as e:
                pass
    logger.info('Finished running as a reader')


def _test_lock_main():
    """\
    SimpleLockインスタンスを用いて実ファイルシステム上で
    排他ロックが動作することを確認する。
    """
    parser = ArgumentParser(description=(__doc__),
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('role', choices=['reader', 'writer'],
                        help='Specify reader or writer')
    parser.add_argument('-c', '--content-path', default='/tmp/content.json',
                        help='Path to content file')
    parser.add_argument('-l', '--lock-path', default='/tmp/content.lock',
                        help='Path to lock file')
    parser.add_argument('-n', type=int, default=10)
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Show debug log')
    args = parser.parse_args()

    logger = getLogger(__name__)
    handler = StreamHandler()
    logger.addHandler(handler)
    if args.debug:
        logger.setLevel(DEBUG)
        handler.setLevel(DEBUG)
    else:
        logger.setLevel(INFO)
        handler.setLevel(INFO)
    handler.setFormatter(Formatter('%(asctime)s %(levelname)7s %(message)s'))
    if args.role == 'reader':
        _test_lock_reader(args.content_path, args.lock_path, args.n, logger)
    else:
        _test_lock_writer(args.content_path, args.lock_path, args.n, logger)
    return 0


if __name__ == '__main__':
    sys.exit(_test_lock_main())
