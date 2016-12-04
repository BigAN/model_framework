# encoding:utf8
from __future__ import unicode_literals
import codecs
import os
from predifined_methods import Feature
from utils.CONST import data_dir as root
import utils.CONST as cst
import multiprocessing as mp
import optparse

FEA = Feature()

FEA.config = os.path.join(cst.app_root_path, 'feas_reconstruct')
FEA.read_conf()

max_len = len(FEA.fea_number_dict)


def init_arguments():
    def right_mode(p):
        if p == "test" or p == "train":
            return p
        else:
            raise optparse.OptionValueError('%s is not a mode.' % p)

    parser = optparse.OptionParser()
    parser.add_option('-m', type=str, dest='mode',
                      help="work mode")
    return parser.parse_args()


args = init_arguments()[0]
file = cst.app_file if args.mode == "train" else cst.test_file
data_file = os.path.join(root, file)
print "data_file:",data_file
feature_lines = os.path.join(root, "_".join([cst.app, "features_lines"]))

with codecs.open(data_file, "r", "utf8") as f:
    data = f.readlines()[1:]

label_name = "punish_status"
max_feature_id_num = str(sorted(FEA.feature_dict.values(), key=lambda x: int(x))[-1])
print "max_feature_id_num", max_feature_id_num


def normal(fc, fea_value, fea):
    return FEA.__getattribute__(cst.parse_method(fc.method)[0])(fc.name, fea_value)


def none(fc, fea_value, fea): return


def pair(fc, fea_value, fea):
    fea_name_list = fc.name.split("&")
    fea_value_list = [fea[num - 1] for num in [FEA.fea_number_dict[fea_name] for fea_name in fea_name_list]]
    # print fea_name_list,fea_value_list,FEA.__getattribute__(fc.method)(fea_name_list, fea_value_list)
    return FEA.__getattribute__(fc.method)(fea_name_list, fea_value_list)


def one_line(line):

    with cst.TimeRecord("initial") as _:
        fea = map(lambda x: x.strip(), line.split("\t"))[:len(FEA.fea_number_dict)]
        try:
            one_lable = str(cst.safe_int(fea[FEA.fea_number_dict[label_name] - 1]))
        except Exception as e:
            print e
            print len(FEA.fea_number_dict),len(fea),line
            print FEA.fea_number_dict
            print fea
            return '\t'.join(["0", max_feature_id_num + ":0"])

    def one_fea((n, fea_value)):
        '''
        1. n,fea_value
        2. fea_name or fea_name_list ,fea_value or fea_value_list

        调用获取v
        :return:
        '''

        fea_name = FEA.num_fea_dict[n]
        fc = FEA.fea_conf[fea_name]
        fun_key = {"cate": normal, "origin": normal, "number": normal, "none": none, "pair": pair}
        if fc.name != cst.label_name:
            return fun_key[fc.method.split("#")[0]](fc, fea_value, fea)
    try:
        rs = filter(lambda x: x, map(one_fea, enumerate(fea + [0] * (max_len - len(fea)), start=1)))
        if rs and max_feature_id_num == rs[-1][0]:
            data_line = " ".join(map(lambda x: ":".join(map(str, x)), sorted(rs, key=lambda x: int(x[0]))))
        else:
            data_line = " ".join(map(lambda x: ":".join(map(str, x)), sorted(rs, key=lambda x: int(x[0])))
                                 + [max_feature_id_num + ":0"])
    except Exception as e:
        print e
        return '\t'.join([one_lable, ""])
    else:
        return '\t'.join([one_lable, data_line])


import time

t = time.time()
with cst.TimeRecord("total") as _:
    pool = mp.Pool(32)
    rs = filter(lambda x:x,pool.map(one_line, data))
    with codecs.open(feature_lines, 'w', 'utf8') as f:
        f.write('\n'.join(rs))
