# encoding:utf8
import os
import codecs
from utils.CONST import data_dir as root
import json
import utils.CONST as cst
import arrow
import itertools
from feature_work.convert_vector.predifined_methods import Conf
from utils.CONST import label_name

FEATURE_IDS = "feature_ids"

GOLD_SPLIT = "__"
FEA_ID_SPLIT = "&#&"

feas_path = os.path.join(cst.app_root_path, 'feas')
features_ids_path = os.path.join(root, "_".join([cst.app, "features_ids"]))
feas_reconstruct_path = os.path.join(cst.app_root_path, 'feas_reconstruct')
feature_id = 1
conf_dict = {}

exclude_conf = set(["all_order_ratio__0.0_", ])
# class FEA_


def one_conf(line):
    global conf_dict, feature_id
    conf = Conf(*line.split(","))
    if conf.method == "none" or conf.name == label_name:
        return
    conf_dict[conf.name] = conf
    conf.parse_ars(conf_dict)
    feature_id += len(conf.key_list)
    if conf.status:
        return [fea + FEA_ID_SPLIT + str(fea_id) for fea, fea_id in
                zip(conf.key_list, xrange(feature_id - len(conf.key_list), feature_id, 1)) if
                not max([x in exclude_conf for x in fea.split("#")])]


with codecs.open(feas_reconstruct_path, 'r', 'utf8') as f, codecs.open(features_ids_path, 'wb', 'utf8') as f_w:
    feature_id_list = list(itertools.chain(*filter(lambda x: x, map(one_conf, f.readlines()))))
    rs = filter(lambda x: not max([c in x for c in list(exclude_conf)]), feature_id_list)
    f_w.write("\n".join(rs))

# print "\n".join(rs)
print "after filter features number is {0},before filter,number features are {1}".format(len(rs), len(feature_id_list))
