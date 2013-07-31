Malhar
======

Malhar operator and codec library

Malhar repository contains open source operator and codec library that can be 
used with the DataTorrent platform to build Realtime streaming applications. In
addition to the library there are contrib, demos and samples. Contrib contains
operators to interface with third-party softwares. Demos contain demo applications and samples contain some sample code on how to use the library.


Getting the source code
------------------------

The Malhar repository is on github.com. If you already have a github account you can git pull the DataTorrent/Malhar repository. If you do not have a github account please create one and pull the repository.

If you would like to make contributons you can fork the repository and make pull requests.

Compiling the code
----------------------

Maven is needed to compile the code. In addition the code has a dependency to the DataTorrent API. The API is available in the DataTorrent maven repository. The location of the repository is 

https://www.datatorrent.com/maven/content/repositories/releases/ 

This location can be specified in maven settings file $HOME/.m2/settings.xml. Below is a sample specification

<settings> 
 <profiles> 
  <profile> 
   <id>datatorrent-repo</id> 
   <activation> 
      <activeByDefault>true</activeByDefault> 
   </activation> 
   <repositories> 
    <repository> 
      <id>datatorrent</id> 
      <name>DataTorrent Release Repository</name> 
      <url>https://www.datatorrent.com/maven/content/repositories/releases/</url> 
    </repository> 
   </repositories> 
  </profile> 
 </profiles> 
</settings>  

Running the code
-------------------

To run the code the DataTorrent platform is needed. The developer version or an evaluation version both of which are free can be downloaded from 

https://www.datatorrent.com/download.php

Please follow the instructions in the software README on how to run the applications.

Discussion group
--------------------

A discussion google group malhar-users@googlegroups.com is available for the 
users of this repository. The subscription to the group is open and postings 
are welcome. You can post anything about the operators, discuss new operator
ideas or report issues and get answers from experts.

Issue tracking
--------------------

The Github issues tracking for the repository is used for tracking issues. You
can monitor the existing issues and their progress there. 
