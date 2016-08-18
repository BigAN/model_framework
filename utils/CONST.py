import os
import argparse
import time

# data
data_dir = "/Users/dongjian/data"

# code
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
feature_ext_root_path = os.path.join(root_path, "feature_work")

# install
cvt_root_path = os.path.join(feature_ext_root_path, "convert_vector")
config_path = os.path.join(feature_ext_root_path, "config")

app_name = "user_features"
version = "v_27_zhenghe_pingjia_jizhong"
app = "_".join([app_name, version])
app_file = "user_feature_raw_dup"  # user_feature_raw_dup user_features_test
test_file = "user_features_test"  # user_features_test
app_root_path = os.path.join(config_path, app_name)
label_name = "punish_status"


def wash(value):
    if value == "NULL":
        value = 0
    value = float(value)
    return 0 if value < 0 or value > 50000000 else value


class TimeRecord(object):
    t = None

    def __init__(self, name):
        self.t = time.time()
        self.name = name

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        # print "{name}   cost {time} seconds".format(**{"name": self.name, "time": time.time() - self.t})
        pass


def safe_get(tmp_list, index, default):
    try:
        return tmp_list[index]
    except IndexError:
        return default


def parse_method(method):
    items = method.split("#")

    if safe_get(items, 1, None) == None:
        items.append("freq")

    if safe_get(items, 2, None) == None:
        items.append(12)

    items[2] = int(items[2])
    key, value, fraction = items
    return key, value, fraction


def init_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('-app', type=str, dest="app",
                        help="app name")
    parser.add_argument('-f', type=str, dest='app_file',
                        help="app file")
    return parser.parse_args()


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


if __name__ == "__main__":
    # global app,app_file
    args = init_arguments()
    app = args.app
    app_file = args.app_file
    print app, app_file
