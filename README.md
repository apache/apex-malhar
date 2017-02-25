Apache Apex Malhar
===============================

Malhar repository contains open source operator and codec library that can be used with the Apache Apex platform to build Realtime streaming applications. In addition to the library there are benchmark, contrib, examples and samples folders available. Examples contain applications built using the library operators. Benchmark contains performance testing applications. Contrib contains additional operators that interface with third party softwares. Samples contain some sample code that shows how to use the library operators.

Contributing
------------

This project welcomes new contributors.  If you would like to help by adding new features, enhancing existing features, or fixing bugs, check out the [contributing guidelines](http://apex.apache.org/contributing.html) and guidelines for [contributing to the operator library](http://apex.apache.org/malhar-contributing.html). 

You acknowledge that your submissions to this repository are made pursuant the terms of the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html) and constitute "Contributions," as defined therein, and you represent and warrant that you have the right and authority to do so.

You can submit new issues and track the progress of existing issues at https://issues.apache.org/jira/browse/APEXMALHAR

Documentation
-------------

Please visit the [documentation section](http://apex.apache.org/docs.html).

Discussion group
--------------------

Please visit http://apex.apache.org and [subscribe](http://apex.apache.org/community.html) to the mailing lists.

Building the project
--------------------

The project uses Maven for the build. Run
```
mvn install -DskipTests
```

The code depends on the Apex API, which is available in the Maven Central Repository.  Individual modules such as library can be built independently by changing directory to the module and running maven there.

Multiple [example applications](examples/) are provided to showcase the Apex platform and application development process. 

License
--------------------

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
