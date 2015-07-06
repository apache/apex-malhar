Malhar
======
Malhar repository contains open source operator and codec library that can be used with the DataTorrent platform to build Realtime streaming applications. In addition to the library there are benchmark, contrib, demos, webdemos and samples folders available. Demos contain demo applications built using the library operators. Webdemos contain webpages for the demos. Benchmark contains performance testing applications. Contrib contains additional operators that interface with third party softwares. Samples contain some sample code that shows how to use the library operators.


Contributing
------------

This project welcomes new contributors.  If you would like to help make Malhar better by adding new features, enhancing existing features, or fixing bugs, here is how to do it.

You acknowledge that your submissions to DataTorrent on this repository are made pursuant the terms of the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html) and constitute "Contributions," as defined therein, and you represent and warrant that you have the right and authority to do so.

  * Fork Malhar into your own GitHub repository
  * Create a topic branch with an appropriate name
  * Write code, comments, tests in your repository
  * Create a GitHub pull request from your repository, providing as many details about your changes as possible
  * After we review and accept your request we’ll commit your code to the DataTorrent/Malhar repository

The submitted code must follow certain prescribed conventions and it is also recommended that it follow the prescribed style. The conventions and style are described in the [Coding Conventions and Style](docs/CodingConventionsAndStyle.md) document.

When adding **new files**, please include the following Apache v2.0 license header at the top of the file, with the fields enclosed by brackets "[]" replaced with your own identifying information. **(Don't include the brackets!)**:

    /*
     * Copyright (c) [XXXX] [NAME OF COPYRIGHT OWNER]
     *
     * Licensed under the Apache License, Version 2.0 (the "License");
     * you may not use this file except in compliance with the License.
     * You may obtain a copy of the License at
     *
     *   http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */

Thanks for contributing!


Documentation
-------------

The documentation for Operators, Widgets, Demos, and Apps is available at [Malhar Javadocs](https://datatorrent.com/docs/apidocs/). Documentation
for the platform itself can be found at [Resources](https://datatorrent.com/resources.php).

Discussion group
--------------------

Please use the [Malhar discussion group](http://groups.google.com/group/malhar-users) for support. Subscription to the group is open and postings are welcome. You can post anything about the operators, discuss new operator ideas or report issues and get answers from experts. You can signup by going to the following url https://groups.google.com/forum/#!forum/malhar-users


Getting the source code
------------------------

The Malhar repository is on github.com. You can download or clone it using the GitHub links.  If you already have a GitHub account you can fork the DataTorrent/Malhar repository and contribute with pull requests.  See Contributing section below.


Compiling the code
----------------------

The project uses maven to build the code. To build the code run mvn install at the top level. The code also has a dependency to the DataTorrent API. The API releases are available in the DataTorrent maven repository and the Malhar pom.xml is configured with it. Individual modules such as library can be built independently by changing directory to the module and running maven there.

Running the code
-------------------

To run the code DataTorrent platform is needed. The developer version or an evaluation version, both of which are free, can be downloaded from 

https://www.datatorrent.com/download/

Please follow the instructions in the software README on how to run the application.  

For an easer setup, sandbox virtual machine provides pre-configured DataTorrent platform with all necessary dependencies, and can be found in the download section.


Demos
-------------------

Multiple demos are provided with the DataTorrent platform to showcase the platform and provide application development examples.  Demos can be executed after downloading and installing DataTorrent platform or the sandbox with pre-configured DataTorrent platform environment.  See [demos source code](https://github.com/DataTorrent/Malhar/tree/master/demos/src/main/java/com/datatorrent/demos) for application development examples.

Web Apps
-------------------

Web interface for the following demos is available:
- Twitter
- Mobile
- Machine Generated Data
- Ads Dimensions
- Fraud

Web apps run on Node.js. Please see [webapps](https://github.com/DataTorrent/Malhar/tree/master/webapps) for more information.

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
