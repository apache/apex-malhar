#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import os
from setuptools import setup, find_packages

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()
DOC_DIR = "doc"
DIST_DIR = "dist"

setup(
    name = "pyapex",
    version = "0.0.4",
    description = ("Python Source code for Apache Apex"),
    license = "Apache License 2.0",
    packages=['pyapex','pyapex.runtime','pyapex.functions'],
    package_dir={"": "src"},
    long_description=read('README'),
    python_requires='~=2.7',
    classifiers=[
        "Development Status :: 1 - Beta",
        "Topic :: Python Support",
        "License :: Apache License 2.0",
    ],
)
