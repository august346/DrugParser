import asyncio
import json
from functools import reduce


async def update_data_batch(in_queue: asyncio.Queue, out_queue: asyncio.Queue, rule_key):
    rule = _get_rule(rule_key)
    while (data_batch := await in_queue.get()) is not None:
        update_batch = [
            rule(item)
            for item in data_batch
        ]
        await out_queue.put(update_batch)
    await out_queue.put(None)


def _get_rule(key):
    return {
        'o3': extract_o3_data
    }.get(key)


def extract_o3_data(edited_dict):
    return _o3_extract_data(
        edited_dict,
        *map(_get_param, ('id', 'name', 'sku', 'mnn_ru')),
        *map(_get_deeper_param, (
            ('manufacturer_id', ('manufacturer_id', 'option_id')),
            ('manufacturer', ('manufacturer_id', 'label')),
            ('manufacturer_ru', ('manufacturer_ru', 'label')),
            ('price', ('price', 'regularPrice', 'amount', 'value'))
        )),
        ('categories', lambda src: src.get('breadcrumbs'), _o3_edit_categories),
        ('receipt', lambda src: src.get('promo_label'), lambda x: 'receipt' in x),
        ('spec_attributes', lambda src: src.get('specification_set_attributes'), _o3_extract_attrs),
        ('desc_attributes', lambda src: src.get('description_set_attributes'), _o3_extract_attrs),
    )


def _get_param(key):
    return key, lambda x: x.get(key), None


def _get_deeper_param(args):
    key, path = args

    def get_deeper(x, x_keys):
        if not x_keys:
            return x
        if x in (None, (), [], {}):
            return None
        return get_deeper(x.get(x_keys[0]), x_keys[1:])

    return key, lambda x: get_deeper(x, path), None


def _o3_extract_data(src_dict, *update_parts_args):
    return {
        key: (update_func or (lambda x: x))(extracted)
        for key, extract_func, update_func in update_parts_args
        if (extracted := extract_func(src_dict)) is not None
    }


def _o3_extract_attrs(extracted):
    def _o3_extract_one(attr_struct):
        attr_label, values = map(attr_struct.get, ('attribute_label', 'values'))
        values = tuple(value.get('value') for value in values)
        return attr_label, values
    return dict(map(_o3_extract_one, extracted))


def _o3_edit_categories(extracted):
    def extract_category(category_struct):
        if (category_id := category_struct.get('id')) in ('2669', '2672', '2671'):
            return None
        return category_id, category_struct.get('name')

    def add_categories(dict_to_upd: dict, categories_struct: dict) -> dict:
        dict_to_upd.update(filter(
            lambda x: x is not None,
            map(extract_category, categories_struct.get('path'))
        ))
        return dict_to_upd

    return reduce(add_categories, json.loads(extracted), dict())
