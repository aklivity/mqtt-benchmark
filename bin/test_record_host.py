import ast
import os
import unittest

SOURCE = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'record-host.py')).read()
tree = ast.parse(SOURCE)
wanted = [n for n in tree.body if (isinstance(n, ast.Assign) and any(getattr(t, 'id', '') == 'MAP_CLASSES' for t in n.targets))
          or (isinstance(n, ast.FunctionDef) and n.name == 'map_stats')]
namespace = {}
exec(compile(ast.Module(body=wanted, type_ignores=[]), 'record-host.py', 'exec'), namespace)
map_stats = namespace['map_stats']

MAPS = [
    '7f0000000000-7f0000100000 rw-s 00000000 00:2b 11 /dev/shm/zilla-engine/data0',
    '7f0000100000-7f0000200000 rw-s 00000000 00:2b 11 /dev/shm/zilla-engine/data0',
    '7f0000200000-7f0000800000 rw-s 00000000 00:2b 12 /dev/shm/zilla-produce/south/mqtt-devices-0-3/00000000000000000000.log',
    '7f0000800000-7f0000900000 rw-s 00000000 00:2b 13 /dev/shm/zilla-produce/south/mqtt-devices-0-3/00000000000000000000.index (deleted)',
    '7f0001000000-7f0002000000 rw-s 00000000 fd:01 14 /var/run/zilla/cache/kafka/mqtt-devices-0/00000000000000000000.log',
    '7f0003000000-7f0003100000 r-xp 00000000 fd:01 15 /usr/lib/libc.so.6',
    '7f0004000000-7f0004100000 rw-p 00000000 00:00 0 ',
    '7f0005000000-7f0005100000 rw-p 00000000 00:00 0 [heap]',
]


class MapStatsTest(unittest.TestCase):
    def test_counts_files_classes_and_deleted_mappings(self):
        cols = dict(c.split('=') for c in map_stats(MAPS))
        self.assertEqual(cols['mappings'], '6')                 # anonymous and [heap] lines are not file-backed
        self.assertEqual(cols['deleted_mappings'], '1')
        self.assertEqual(cols['deleted_files'], '1')
        self.assertEqual(cols['engine_files'], '1')             # the same file mapped twice counts once
        self.assertEqual(cols['engine_mib'], '2')
        self.assertEqual(cols['produce_files'], '2')
        self.assertEqual(cols['produce_mib'], '7')
        self.assertEqual(cols['cache_mib'], '16')
        self.assertEqual(cols['other_files'], '1')

    def test_empty(self):
        cols = dict(c.split('=') for c in map_stats([]))
        self.assertEqual(cols['mappings'], '0')


if __name__ == '__main__':
    unittest.main()
