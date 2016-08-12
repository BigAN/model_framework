#encoding:utf8
import re


def mul_replace(data, rep=None):
    if not rep:
        rep = {"\n\r": "",
               "\n": "",
               "\r": "",
               ',': ";",
               '，': ";",
               "\&nbsp;": "",
               ' ': ""}
    rep = dict((re.escape(k), v) for k, v in rep.iteritems())
    pattern = re.compile("|".join(rep.keys()))
    return pattern.sub(lambda m: rep[re.escape(m.group(0))], data)


a = "a,b#c"

print mul_replace(a,{",":"","#":""})
