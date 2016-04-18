# encoding:utf8
import os
import codecs

from utils.CONST import data_dir as root
import json
import utils.CONST as cst
import arrow
import itertools

config = os.path.join(cst.app_root_path, 'feas')

GOLD_SPLIT = "__"
FEA_ID_SPLIT = "&#&"
FEA_SPILT = "&"


def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")


def gen_cates(method, arrs):
    def cate(arrs):
        return {ar: 1 for ar in arrs.split("#")}

    def number(arrs):
        if len(arrs) <= 1: return []
        arrs_list = [float(x) for x in arrs.split("#")]
        return arrs_list

    def pair(arrs):
        return []

    def none(arrs):
        return

    return locals().get(method)(arrs)


def trans_value_to_threds(threds, value):
    threds = sorted(threds, key=lambda x: float(x))
    bigram = [(float(threds[n]), float(threds[n + 1])) for n in xrange(0, len(threds) - 1)]
    if len(threds) == 1:
        return threds[0], threds[0]
    for l, h in bigram:
        if l <= value < h:
            return l, h
    else:
        if value >= threds[-1]:
            return threds[-2], threds[-1]
        if value < threds[0]:
            return threds[0], threds[1]


class Conf(object):
    def __init__(self, name, method, status, ars):
        self.name = name
        self.method = method
        self.status = str2bool(status)
        self.ars = ars.strip()
        self.values_list = None
        self.feature_list = None
        self.key = None

    def __str__(self):
        return "_".join(map(str, [self.name, self.method, self.status, self.ars]))

    def parse_ars(self, conf_dict):
        def number(ars):
            self.values_list = ars.split("#")
            self.feature_list = ["_".join([self.values_list[ar], self.values_list[ar + 1]]) for ar in
                                 xrange(0, len(self.values_list) - 1)]
            self.key_list = ["__".join([self.name,fea_value])for fea_value in self.feature_list]

        def cate(ars):
            self.values_list = ars.split("#")
            self.feature_list = self.values_list
            self.key_list = ["__".join([self.name,fea_value])for fea_value in self.feature_list]

        def pair(ars):
            fea_name_list = self.name.split("&")
            fea_key_list = [conf_dict[name].key_list for name in fea_name_list]
            product_list = itertools.product(*fea_key_list)
            self.key_list = ["#".join(x) for x in product_list]
            self.name = "#".join(fea_name_list)

        def none(ars):
            pass

        locals().get(self.method.split("#")[0])(self.ars)


class Feature(object):
    feature_number = 0
    feature_dict = {}
    fea_id_out = os.path.join(root, "_".join([cst.app, "features_ids"]))
    fea_conf = {}
    fea_number_dict = {}
    config = config
    lock = True

    def __init__(self):
        # self.read_conf()
        self._init_feature_dict()

    def _init_feature_dict(self):
        if os.path.isfile(self.fea_id_out):
            with codecs.open(self.fea_id_out) as file:
                def one_fea(data):
                    fea_name, key = data.split(FEA_ID_SPLIT)
                    self.feature_dict[fea_name.strip()] = key.strip()

                map(one_fea, file.readlines())
                print self.feature_dict
            try:
                self.feature_number = int(sorted(self.feature_dict.values(), key=lambda x: int(x), reverse=True)[0])
            except:
                pass

    def read_conf(self):
        with codecs.open(self.config, 'r', 'utf8') as f:
            for n, l in enumerate(f.readlines()):
                if not l.startswith("#"):  # 过掉注释
                    n += 1
                    print l
                    cf = Conf(*l.split(","))
                    cf.arrs_list = gen_cates(cst.parse_method(cf.method)[0], cf.ars)
                    self.fea_conf[cf.name] = cf
                    self.fea_number_dict[cf.name] = n
        self.num_fea_dict = {self.fea_number_dict[x]: x for x in self.fea_number_dict}
        self.fea_number_value_list = {x: list() for x in self.fea_number_dict}

    def add_feature(self, fea_name):
        # print fea_name,fea_name in self.feature_dict

        if fea_name in self.feature_dict:
            return self.feature_dict[fea_name]
        elif not self.lock:
            self.feature_number += 1
            self.feature_dict[fea_name] = self.feature_number
        else:
            return None
        return self.feature_number

    def cate(self, fea_name, fea_value):
        fea_value = "0" if fea_value == "NULL" else fea_value
        k = fea_name + GOLD_SPLIT + fea_value
        return self.add_feature(k)

    def number(self, fea_name, fea_value):
        def wash(value):
            if value == "NULL":
                value = 0
            value = float(value)
            return 0 if value < 0 or value > 50000000 else value

        fea_value = wash(fea_value)
        cf = self.fea_conf[fea_name]
        threds = [float(x) for x in cf.ars.strip().split("#")]
        l, h = trans_value_to_threds(threds, float(fea_value))
        k = GOLD_SPLIT.join(map(str, [fea_name, "_".join(map(lambda x: str(float('%0.3f' % float(x))), [l, h]))]))
        # print "number",k,self.add_feature(k)
        return self.add_feature(k)

    def pair(self, fea_name_list, fea_value_list):

        def check_value(fea_value):
            fea_value = fea_value.strip()
            return "0" if fea_value == "NULL" or fea_value <= "0" else fea_value

        fea_value_list = [check_value(val) for val in fea_value_list]
        # 获得参数list
        fea_list = fea_name_list
        cf_list = [self.fea_conf[fea] for fea in fea_list]
        # 做pair
        fea_name_list = [cf.name for cf in cf_list]

        def one_fea_value((cf, fea_value)):
            fea_value = float(fea_value.strip())
            data_method = cf.method.split("#")[0]
            if data_method == "number":
                # print cf.arrs_list, fea_value
                # print trans_value_to_threds(cf.arrs_list, fea_value)
                return "_".join([str(x) for x in trans_value_to_threds(cf.arrs_list, fea_value)])

            if data_method == "cate":
                return str(fea_value)

        value_name_list = map(one_fea_value, zip(cf_list, fea_value_list))
        key = "#".join(["__".join([fea,value]) for fea,value in zip(fea_name_list,value_name_list)])
        # print "pair",key,self.add_feature(key)
        return self.add_feature(key)

    def origin(self, fea_name, fea_value):
        k = fea_name
        if k in self.feature_dict:
            return self.feature_dict[k]
        else:
            self.feature_number += 1
            self.feature_dict[k] = self.feature_number
        return self.feature_number

    def print_feas(self):
        with codecs.open(self.fea_id_out, 'wb', 'utf8') as f:
            f.truncate()
            f.write("\n".join(
                    sorted([k.strip() + "&#&" + str(v).strip() for k, v in self.feature_dict.items()],
                           key=lambda x: x.split("&#&")[1])))


if __name__ == "__main__":
    f = Feature()
    f.read_conf()
    import json

    print json.dumps(f.num_fea_dict, indent=4)
