#!/usr/bin/python3

import subprocess
import re
import os
import shutil
import json
import binascii
import time


from robot.api.deco import keyword
from robot.api import logger

import robot.errors
import requests
import uuid
from robot.libraries.BuiltIn import BuiltIn

ROBOT_AUTO_KEYWORDS = False

@keyword('Prepare Environment')
def prepare_environment():
    return 


@keyword('Cleanup Environment')
def cleanup_environment():
    return 
    
