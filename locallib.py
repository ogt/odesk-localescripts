# coding: utf-8
from __future__ import unicode_literals
import os
import ConfigParser


CONFIG_INI = os.path.dirname(os.path.abspath(__file__)) + '/config.ini'


def get_config():
    config = ConfigParser.ConfigParser()
    config.read(CONFIG_INI)
    return config


