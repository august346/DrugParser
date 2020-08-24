import unittest

from pharmacy import editor


class TestExtractor(unittest.TestCase):

    def test_extractor(self):
        example_dict = {
            '0': None,
            'a': 1,
            'b': 0,
            'c': 'c'
        }

        actual_result = editor._o3_extract_data(
            example_dict,
            ('0', lambda x: x['0'], None),
            ('a', lambda x: x['a'], lambda x: x // 2),
            ('b', lambda x: x['b'], lambda x: [x, 1]),
            ('c', lambda x: x['c'], lambda x: {'c': x})
        )

        self.assertEqual(
            {
                'a': 0,
                'b': [0, 1],
                'c': {'c': 'c'}
            },
            actual_result
        )

    def test_o3_extractor(self):
        example_dict = {
            'id': 2345,
            'name': 'drug_name',
            'sku': '123',
            'trash': None,
            'manufacturer_id': {
                'label': 'drug_manufacturer'
            },
            'description_set_attributes': [
                {"attribute_label": "k0", "values": [{"value": "v0"}]},
                {"attribute_label": "k1", "values": [{"value": "v1"}]},
            ]
        }

        self.assertEqual(
            {
                'id': 2345,
                'name': 'drug_name',
                'sku': '123',
                'manufacturer': 'drug_manufacturer',
                'desc_attributes': {
                    'k0': ('v0',),
                    'k1': ('v1',),
                }
            },
            editor.extract_o3_data(example_dict)
        )
