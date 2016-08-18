# encoding:utf8
import codecs
import os
import math
from utils.CONST import data_dir as root
import utils.CONST as cst
import multiprocessing as mp
import re

feature_ids_path = os.path.join(root, "_".join([cst.app, "features_ids"]))
model_xishu_ids_sort = os.path.join(root, "model_xishu_ids_sort_"+cst.version)
feature_lines_path = os.path.join(root, "_".join([cst.app, "features_lines"]))
feature_lines_with_info = os.path.join(root, "_".join([cst.app, "features_lines_with_info"]))
# 读取所有特征
with codecs.open(feature_lines_path, 'r', 'utf8') as f:
    def one_fea(fea):
        fsp = fea.split('\t')
        label, features = fsp[0], [key.split(":")[0] for key in fsp[1].split(" ") if key.split(":")[1]=="1"]
        return label, features


    feature_lines = map(one_fea, f.readlines()[:])
# print feature_lines
# 读取字典
with codecs.open(model_xishu_ids_sort, 'r', 'utf8') as f:
    def one_conf(data):
        return map(lambda x: str(x).strip(), re.split("&#&|\t", data))


    tuple = filter(lambda x: len(x) > 2 and len(x[2]) > 0, map(one_conf, f.readlines()))
    key_dict = {t[1]: (t[0], t[1], t[2]) for t in tuple}
    name_dict = {t[0]: (t[0], t[1], t[2]) for t in tuple}
# import json
# print json.dumps(key_dict,indent=4)
# print len(key_dict)
with codecs.open(feature_lines_with_info, 'w', 'utf8') as f:
    def one_line(data):
        def one_ele(ele):
            val = key_dict.get(ele, None)
            if val :  # val 不为空 且 系数不为0
                return "___".join([val[0], val[-1]])
        try:
            return "\t".join(
                sorted(filter(lambda x: x, map(one_ele, data[1])), key=lambda x: -float(x.split("___")[1])))
        except Exception as e:
            print map(one_ele, data[1])
            raise e
    import multiprocessing as mp
    pool = mp.Pool(32)
    rs = pool.map(one_line, feature_lines)
    # print rs[0]
    f.write('\n'.join(rs))
    print len(feature_lines),len(rs)
    print feature_lines[-1],rs[-1]
