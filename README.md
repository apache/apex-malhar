Apache Apex Malhar (incubating)
===============================

Malhar repository contains open source operator and codec library that can be used with the Apache Apex (incubating) platform to build Realtime streaming applications. In addition to the library there are benchmark, contrib, demos, webdemos and samples folders available. Demos contain demo applications built using the library operators. Webdemos contain webpages for the demos. Benchmark contains performance testing applications. Contrib contains additional operators that interface with third party softwares. Samples contain some sample code that shows how to use the library operators.

Contributing
------------

This project welcomes new contributors.  If you would like to help make Malhar better by adding new features, enhancing existing features, or fixing bugs, here is how to do it.

You acknowledge that your submissions on this repository are made pursuant the terms of the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html) and constitute "Contributions," as defined therein, and you represent and warrant that you have the right and authority to do so.

  * Fork Malhar into your own GitHub repository
  * Create a topic branch with an appropriate name
  * Write code, comments, tests in your repository
  * Create a GitHub pull request from your repository, providing as many details about your changes as possible
  * After we review and accept your request we’ll commit your code to the repository

The submitted code must follow certain prescribed conventions and it is also recommended that it follow the prescribed style. The conventions and style are described in the [Coding Conventions and Style](docs/CodingConventionsAndStyle.md) document.

When adding **new files**, please include the Apache v2.0 license header. From the top level directory:

Run `mvn license:check -Dlicense.skip=false` to check correct header formatting.
Run `mvn license:format -Dlicense.skip=false` to automatically add the header when missing.

Thanks for contributing!

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

The code depends on the Apex API. The API releases are available in the DataTorrent maven repository and the Malhar pom.xml is configured with it. Individual modules such as library can be built independently by changing directory to the module and running maven there.

Multiple [demo applications](demos/src/main/java/com/datatorrent/demos) are provided to showcase the Apex platform and application development process. 

Issue tracking
--------------------

[Malhar JIRA](https://malhar.atlassian.net/projects/MLHR) issue tracking system is used for this project.
You can submit new issues and track the progress of existing issues at https://malhar.atlassian.net/projects/MLHR.

When working with JIRA to submit pull requests, please use [smart commits](https://confluence.atlassian.com/display/AOD/Processing+JIRA+issues+with+commit+messages) feature by specifying MLHR-XXXX in the commit messages.
It helps us link commits with issues being tracked for easy reference.  And example commit might look like this:

    git commit -am "MLHR-1234 #comment Task completed ahead of schedule #resolve"


License
--------------------

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
