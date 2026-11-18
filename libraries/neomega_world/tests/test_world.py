"""Unit tests for neomega_world pure data models and bounds validation."""
import unittest

from neomega_world import (
    MAX_COMMAND_BYTES,
    MAX_CUSTOM_NAME_BYTES,
    Region,
    canonical,
    container_slots,
    differences,
    digest,
    normalize_block,
    position,
    region_snapshot,
    restore_difference,
    snapshot,
    validate_command_block_fields,
    verify_snapshot,
)


class PositionAndRegionTests(unittest.TestCase):
    def test_position_bounds_and_types(self):
        self.assertEqual(position([0, -64, 100]), (0, -64, 100))
        for bad in (
            [0, 0],
            [0, 0, 0, 0],
            [True, 0, 0],
            [1.0, 0, 0],
            ['0', 0, 0],
            [-(2**31) - 1, 0, 0],
            [2**31, 0, 0],
        ):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    position(bad)

    def test_region_volume_tiles_and_overlaps(self):
        region = Region((0, 0, 0), (3, 2, 2))
        self.assertEqual(region.volume, 12)
        tiles = list(region.tiles(edge=2))
        self.assertEqual([t.as_dict() for t in tiles], [
            {'origin': [0, 0, 0], 'size': [2, 2, 2], 'volume': 8},
            {'origin': [2, 0, 0], 'size': [1, 2, 2], 'volume': 4},
        ])
        self.assertTrue(region.overlaps(Region((2, 1, 1), (2, 2, 2))))
        self.assertFalse(region.overlaps(Region((3, 0, 0), (1, 1, 1))))

        with self.assertRaises(ValueError):
            Region((0, 0, 0), (0, 1, 1))
        with self.assertRaises(ValueError):
            Region((2**31 - 1, 0, 0), (2, 1, 1))
        with self.assertRaises(ValueError):
            list(region.tiles(edge=17))
        with self.assertRaises(ValueError):
            list(region.tiles(edge=True))


class ContainerSlotsTests(unittest.TestCase):
    def test_valid_single_and_double_chest_slots(self):
        item_a = {'item': {'name': 'minecraft:stone', 'val': 0}, 'count': 1}
        item_b = {'item': {'name': 'minecraft:dirt', 'val': 0}, 'count': 2}
        self.assertEqual(container_slots({0: item_a, '26': item_b}), {'0': item_a, '26': item_b})
        self.assertEqual(
            container_slots({27: item_a, '53': item_b}, double_chest=True),
            {'27': item_a, '53': item_b},
        )

    def test_rejects_duplicate_int_and_str_keys(self):
        item_a = {'item': {'name': 'minecraft:stone', 'val': 0}, 'count': 1}
        item_b = {'item': {'name': 'minecraft:dirt', 'val': 0}, 'count': 2}
        with self.assertRaises(ValueError):
            container_slots({0: item_a, '0': item_b})
        with self.assertRaises(ValueError):
            container_slots({'27': item_a, 27: item_b}, double_chest=True)

    def test_rejects_invalid_keys_and_items(self):
        item = {'item': {'name': 'minecraft:stone', 'val': 0}, 'count': 1}
        for bad in (
            'not-a-dict',
            {'00': item},
            {' 0': item},
            {'+0': item},
            {'²': item},
            {'٠': item},
            {True: item},
            {1.0: item},
            {-1: item},
            {27: item},
            {0: 'not-a-dict'},
            {0: {'count': float('nan')}},
        ):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    container_slots(bad)


class NormalizeBlockCommandValidationTests(unittest.TestCase):
    def test_valid_command_block(self):
        cell = normalize_block({
            'foreground': {
                'name': 'minecraft:repeating_command_block',
                'states': {'facing_direction': 5, 'conditional_bit': 1},
            },
            'entity_data': {
                'Command': 'say hi',
                'CustomName': 'helper',
                'TickDelay': 20,
                'auto': 1,
                'TrackOutput': 0,
                'ExecuteOnFirstTick': True,
            },
        })
        self.assertEqual(cell['coverage'], ['foreground', 'command_configuration'])
        self.assertEqual(cell['missing'], [])
        self.assertEqual(cell['nbt'], {
            'kind': 'command_block',
            'fields': {
                'command': 'say hi',
                'mode': 1,
                'facing': 5,
                'conditional': True,
                'need_redstone': False,
                'tick_delay': 20,
                'name': 'helper',
                'track_output': False,
                'execute_on_first_tick': True,
            },
        })

    def test_rejects_invalid_command_custom_name_tick_delay_and_facing(self):
        cases = [
            ('Command=None', {'Command': None}, {}),
            ('Command=int', {'Command': 123}, {}),
            ('Command=newline', {'Command': 'say a\nsay b'}, {}),
            ('Command=cr', {'Command': 'say a\rsay b'}, {}),
            ('Command=nul', {'Command': 'say a\x00'}, {}),
            ('Command=too_long', {'Command': 'a' * (MAX_COMMAND_BYTES + 1)}, {}),
            ('CustomName=None', {'CustomName': None}, {}),
            ('CustomName=int', {'CustomName': 1}, {}),
            ('CustomName=too_long_utf8', {'CustomName': '中' * 342}, {}),
            ('TickDelay=None', {'TickDelay': None}, {}),
            ('TickDelay=bool', {'TickDelay': True}, {}),
            ('TickDelay=str', {'TickDelay': '5'}, {}),
            ('TickDelay=neg', {'TickDelay': -1}, {}),
            ('TickDelay=overflow', {'TickDelay': 2**31}, {}),
            ('facing=None', {}, {'facing_direction': None}),
            ('facing=bool', {}, {'facing_direction': False}),
            ('facing=str', {}, {'facing_direction': '0'}),
            ('facing=neg', {}, {'facing_direction': -1}),
            ('facing=6', {}, {'facing_direction': 6}),
            ('TrackOutput=bad', {'TrackOutput': 'yes'}, {}),
            ('auto=2', {'auto': 2}, {}),
            ('conditional_bit=None', {}, {'conditional_bit': None}),
        ]
        for label, entity_overrides, state_overrides in cases:
            with self.subTest(label=label):
                states = {'facing_direction': 0}
                states.update(state_overrides)
                entity = {'Command': 'say ok', 'CustomName': 'ok', 'TickDelay': 0}
                entity.update(entity_overrides)
                cell = normalize_block({
                    'foreground': {'name': 'minecraft:command_block', 'states': states},
                    'entity_data': entity,
                })
                self.assertIsNone(cell['nbt'])
                self.assertNotIn('command_configuration', cell['coverage'])
                self.assertIn('command_configuration', cell['missing'])

        with self.assertRaises(ValueError):
            validate_command_block_fields({
                'command': 'say ok', 'mode': 0, 'facing': 0, 'tick_delay': 0,
                'name': 'a' * (MAX_CUSTOM_NAME_BYTES + 1),
                'conditional': False, 'need_redstone': True, 'track_output': False,
                'execute_on_first_tick': False,
            })


class SnapshotAndContainerNormalizeTests(unittest.TestCase):
    def test_container_normalize_and_duplicate_slot_loss(self):
        ok = normalize_block({
            'foreground': {'name': 'minecraft:chest', 'states': {}},
            'entity_data': {'Items': [{'Name': 'minecraft:apple', 'Count': 2, 'Damage': 0, 'Slot': 1}]},
        })
        self.assertIn('container_items', ok['coverage'])
        self.assertEqual(ok['nbt'], {
            'kind': 'container',
            'slots': {'1': {'item': {'name': 'minecraft:apple', 'val': 0}, 'count': 2}},
        })

        dup = normalize_block({
            'foreground': {'name': 'minecraft:chest', 'states': {}},
            'entity_data': {'Items': [
                {'Name': 'minecraft:apple', 'Count': 1, 'Damage': 0, 'Slot': 0},
                {'Name': 'minecraft:stone', 'Count': 1, 'Damage': 0, 'Slot': 0},
            ]},
        })
        self.assertIsNone(dup['nbt'])
        self.assertIn('container_items', dup['missing'])

        paired = normalize_block({
            'foreground': {'name': 'minecraft:chest', 'states': {}},
            'entity_data': {'pairx': 1, 'pairz': 0, 'Items': []},
        })
        self.assertIsNone(paired['nbt'])
        self.assertIn('container_pair_topology', paired['missing'])
        self.assertIn('container_items', paired['missing'])

    def test_snapshot_verify_and_differences(self):
        dump = {
            'pos': [0, 64, 0],
            'size': [1, 1, 1],
            'blocks': [{'abs': [0, 64, 0], 'foreground': {'name': 'minecraft:stone', 'states': {}}}],
        }
        snap = region_snapshot(dump, dimension=0, epoch='ep1')
        body = verify_snapshot(snap)
        self.assertEqual(body['epoch'], 'ep1')
        self.assertEqual(digest(body), snap['sha256'])

        tampered = dict(snap, dimension=1)
        with self.assertRaises(ValueError):
            verify_snapshot(tampered)
        with self.assertRaises(ValueError):
            verify_snapshot('bad')

        diffs = differences({'0,0,0': {'a': 1}}, {'0,0,0': {'a': 2}, '0,0,1': {'a': 1}})
        self.assertEqual([d['state'] for d in diffs], ['changed', 'unknown'])
        self.assertTrue(restore_difference(snap['cells']['0,0,0'], normalize_block({
            'foreground': {'name': 'minecraft:dirt', 'states': {}},
        })))


if __name__ == '__main__':
    unittest.main()
