Apache Apex Malhar (incubating)
===============================

Malhar repository contains open source operator and codec library that can be used with the Apache Apex (incubating) platform to build Realtime streaming applications. In addition to the library there are benchmark, contrib, demos, webdemos and samples folders available. Demos contain demo applications built using the library operators. Webdemos contain webpages for the demos. Benchmark contains performance testing applications. Contrib contains additional operators that interface with third party softwares. Samples contain some sample code that shows how to use the library operators.

Contributing
------------

This project welcomes new contributors.  If you would like to help make Malhar better by adding new features, enhancing existing features, or fixing bugs, check out the [contributing guidelines](http://apex.incubator.apache.org/contributing.html).

You acknowledge that your submissions to this repository are made pursuant the terms of the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html) and constitute "Contributions," as defined therein, and you represent and warrant that you have the right and authority to do so.

Documentation
-------------

Please visit the [documentation section](http://apex.incubator.apache.org/docs.html).

Discussion group
--------------------

Please visit http://apex.incubator.apache.org and [subscribe](http://apex.incubator.apache.org/community.html) to the mailing lists.

Building the project
--------------------

The project uses Maven for the build. Run
```
mvn install
```

The code depends on the Apex API, which is available in the Maven Central Repository.  Individual modules such as library can be built independently by changing directory to the module and running maven there.

Multiple [demo applications](demos/src/main/java/com/datatorrent/demos) are provided to showcase the Apex platform and application development process. 

Issue tracking
--------------------

[JIRA](https://issues.apache.org/jira/browse/APEXMALHAR) issue tracking system is used for this project.
You can submit new issues and track the progress of existing issues at https://issues.apache.org/jira/browse/APEXMALHAR

Please include the JIRA ticket number into the commit messages. It will automatically add the commit message to the JIRA ticket(s) and help link the commit with the issue(s) being tracked for easy reference.
An example commit might look like this:

    git commit -am "APEXMALHAR-1234 Task completed ahead of schedule"

JIRA tickets should be resolved and fix version field set by the committer merging the pull request.

License
--------------------

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
