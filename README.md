Malhar
======

Malhar repository contains open source operator and codec library that can be 
used with the DataTorrent platform to build Realtime streaming applications. In
addition to the library there are contrib, demos and samples available. Demos contain demo applications built using the library operstors. Contrib contains additional operators that interface with third party softwares. Samples contain some sample code that shows how to use the library operators.


Getting the source code
------------------------

The Malhar repository is on github.com. If you already have a github account you can git pull the DataTorrent/Malhar repository. If you do not have a github account please create one and pull the repository.

Please fork the repository and create pull requests to contribute to the project.

Compiling the code
----------------------

The project uses maven to build the code. To build the code run mvn install at the top level. The code also has a dependency to the DataTorrent API. The API releases are available in the DataTorrent maven repository and the Malhar pom.xml is configured with it. Individual modules such as library can be built independently by changing directory to the module and running maven there.

Running the code
-------------------

To run the code DataTorrent platform is needed. The developer version or an evaluation version, both of which are free, can be downloaded from 

https://www.datatorrent.com/download.php

Please follow the instructions in the software README on how to run the application.

Discussion group
--------------------

A google group malhar-users@googlegroups.com is available for discussions. Subscription to the group is open and postings are welcome. You can post anything about the operators, discuss new operator ideas or report issues and get answers from experts.

Issue tracking
--------------------

Github issues tracking is used for tracking issues for this repository. You can monitor the state of existing issues and their track their progress there. 
