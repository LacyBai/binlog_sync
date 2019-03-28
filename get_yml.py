#!/usr/bin/env python
#-*- coding:utf8 -*-
# Power by Long@www.haohaozhu.com 2018-12-15 16:45:29

import yaml

class get_yml:
    def __init__(self):
        pass

    def yml_dict(self, f):
        yml_f = open(f)
        try:
            dataMap = yaml.safe_load(yml_f)
            yml_f.close()
            return dataMap
        except Exception:
            raise "your yaml file is error fomart"
