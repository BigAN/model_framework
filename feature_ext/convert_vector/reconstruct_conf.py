# encoding:utf8
from __future__ import unicode_literals
import codecs
import os
from predifined_methods import Feature
from utils.CONST import data_dir as root
import utils.CONST as cst
import re

FEA = Feature()
FEA.read_conf()

data_file = os.path.join(root, cst.app_file)
feas = os.path.join(root, "_".join([cst.app, "features_lines"]))
rs_file = os.path.join(cst.app_root_path, 'feas_reconstruct')
open(rs_file, 'w').close()

with codecs.open(data_file, "r", "utf8") as f:
    ins = f.readlines()
    data = ins[1:]

label_name = "punish_status"

c_args = {",": "0x32"}


# def mul_replace(c_args):
#
#     return text


#
def print_fea_vector(data, split_fea=True):
    with codecs.open(feas, "w", "utf8") as file:
        for d in data:
            fea = d.split("\t")
            for n, f in enumerate(fea):
                n += 1
                if n != FEA.fea_number_dict[label_name]:
                    fea_name = FEA.num_fea_dict[n]
                    FEA.fea_number_value_list[fea_name].append(f)


print FEA.num_fea_dict


def generate_conf(fea_number_value_list):
    for num, name in FEA.num_fea_dict.items():
        k, v = name, FEA.fea_number_value_list[name]
        conf = FEA.fea_conf[k]
        yield conf


def parse_method(method):
    items = method.split("#")
    if len(items) > 1:
        key, value = items
    else:
        key, value = items[0], "None"
    return key, value


def remove_dup(value_list):
    return list(set(value_list))


def process_cate(conf, value):
    return remove_dup(FEA.fea_number_value_list[conf.name])


def process_number(conf, value):
    v = FEA.fea_number_value_list[conf.name]
    sorted_v = filter(lambda x: float(x) >= 0,
                      sorted([x for x in v if x.strip() != "NULL"], key=lambda x: float(x)))

    def equal_freq(v):
        freq = sorted(list(set([v[index] for index in range(1, len(v), len(v) / 20)] + [v[-1]] )),
                      key = lambda x: float(x))
        return freq

    def equal_dis(v):
        l, h = float(v[0]), float(v[-1])
        l = l if l >= 0 else 0
        return sorted(remove_dup([str(l + i * (h - l) / 10.0) for i in range(10 + 1)]), key=lambda x: float(x))

    data_huafen = locals().get("equal_{key}".format(**{"key": value}))
    if not data_huafen:
        data_huafen = equal_freq
    return data_huafen(sorted_v)


def process_pair(conf, value):
    return []


def one_conf(conf):
    key, value = parse_method(conf.method)
    data_method = globals().get("process_{key}".format(**{"key": key}))
    rs = data_method(conf, value)

    def print_conf(conf, values):
        with codecs.open(rs_file, 'a', 'utf8') as f:
            f.write(
                    ','.join([conf.name, conf.method,
                              '#'.join([v.strip() for v in values]).replace(",", "0x32")]) + '\n')
            print ','.join([conf.name, conf.method, '#'.join([v.strip() for v in values]).replace(",", "0x32")])

    print_conf(conf, rs)


def process(fea_number_value_list):
    print_fea_vector(data)
    confs = [conf for conf in generate_conf(fea_number_value_list)]
    print len(confs)
    map(one_conf, confs)


if __name__ == "__main__":
    process(FEA.fea_number_value_list)
    # print mul_replace(",111,21123")
