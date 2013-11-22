Malhar
======

Malhar repository contains open source operator and codec library that can be used with the DataTorrent platform to build Realtime streaming applications. In addition to the library there are contrib, demos, webdemos and samples folders available. Demos contain demo applications built using the library operators. Webdemos contain webpages for the demos. Contrib contains additional operators that interface with third party softwares. Samples contain some sample code that shows how to use the library operators.

[![Build Status](https://travis-ci.org/DataTorrent/Malhar.png?branch=master)](https://travis-ci.org/DataTorrent/Malhar)

Discussion group
--------------------

The google group malhar-users@googlegroups.com is available for discussions. Subscription to the group is open and postings are welcome. You can post anything about the operators, discuss new operator ideas or report issues and get answers from experts. You can signup by going to the following url https://groups.google.com/forum/#!forum/malhar-users


Getting the source code
------------------------

The Malhar repository is on github.com. You can download or clone it using the GitHub links.  If you already have a GitHub account you can fork the DataTorrent/Malhar repository and contribute with pull requests.  See Contributing section below.


Compiling the code
----------------------

The project uses maven to build the code. To build the code run mvn install at the top level. The code also has a dependency to the DataTorrent API. The API releases are available in the DataTorrent maven repository and the Malhar pom.xml is configured with it. Individual modules such as library can be built independently by changing directory to the module and running maven there.

Running the code
-------------------

To run the code DataTorrent platform is needed. The developer version or an evaluation version, both of which are free, can be downloaded from 

https://www.datatorrent.com/download.php

Please follow the instructions in the software README on how to run the application.  

For an easer setup, sandbox virtual machine provides pre-configured DataTorrent platform with all necessary dependencies, and can be found in the download section.


Demos
-------------------

Multiple demos are provided with the DataTorrent platform to showcase the platform and provide application development examples.  Demos can be executed after downloading and installing DataTorrent platform or the sandbox with pre-configured DataTorrent platform environment.  See [demos source code](https://github.com/DataTorrent/Malhar/tree/master/demos/src/main/java/com/datatorrent/demos) for application development examples.

Web Demos
-------------------

Web interface for the following demos is available:
- Twitter
- Mobile
- Ads Dimensions
- Site Operations
- Machine Generated Data

Web demos run on Node.js. Please see [webdemos](https://github.com/DataTorrent/Malhar/tree/master/webdemos) for more information.

Issue tracking
--------------------

Github issues tracking is used for tracking issues for this repository. You can monitor the state of existing issues and their track their progress there. 

Contributing
--------------------

If you would like to help make Malhar better by adding new features, enhancing existing features, or fixing bugs, here is how to do it.

  * Fork Malhar into your own GitHub repository
  * Create a topic branch with an appropriate name
  * Write code, comments, tests in your repository
  * Create a GitHub pull request from your repository, providing as many details about your changes as possible
  * After we review and accept your request we’ll commit your code to the DataTorrent/Malhar repository

The submitted code must follow certain prescribed conventions and it is also recommended that it follow the prescribed style. The conventions and style are described in the [Coding Conventions and Style](docs/CodingConventionsAndStyle.md) document.

Thanks for contributing!

License
--------------------

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
