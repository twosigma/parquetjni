..
  Copyright 2020 Two Sigma Investments, LP.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

============
 parquetjni
============

Building
========

.. code-block:: shell

   # From the repository root
   mkdir build
   cd build
   cmake .. \
       -DJAVA_HOME=... \
       -DArrow_DIR=... \
       -DParquet_DIR=... \
       -DGLOG_INCLUDE_DIRS=... \
       -DGLOG_LINK_DIRS=... \
       -DAWSSDK_INCLUDE_DIRS=... \
       -DAWSSDK_LINK_DIRS=...
   make

   # Return to the repository root
   cd ..
   mvn package -Dparquetjni.cpp.build.dir=$(pwd)/build/
