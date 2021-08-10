#!/bin/sh
#
# $Header: rdbms/demo/xstream/java/xsdemo_env.sh /main/2 2013/08/23 03:10:22 luisram Exp $
#
# xsdemo_env.sh
#
# Copyright (c) 2009, 2013, Oracle and/or its affiliates. All rights reserved.
#
#    NAME
#      xsdemo_env.sh - Sets up environmental variables for XSDemo java client
#
#    DESCRIPTION
#      Before compile and run the XSDemo java client:
#        ]$ source xsdemo_env.sh
#
#    NOTES
#      <other useful comments, qualifications, etc.>
#
#    MODIFIED   (MM/DD/YY)
#    luisram     08/22/13 - Bug 17343397 - xstrm: fix xsdemo_env.sh to use
#                           ojdbc6.jar instead of ojdbc5.jar
#    tianli      07/06/09 - Creation
#
########################################
export CLASSPATH=.:${ORACLE_HOME}/rdbms/jlib/xstreams.jar:${ORACLE_HOME}/dbjava/lib/ojdbc6.jar:${CLASSPATH}
echo 'CLASSPATH: ' ${CLASSPATH}
export PATH=.:${ORACLE_HOME}/bin:${ORACLE_HOME}/jdk/bin:${PATH}
echo 'PATH: ' ${PATH}
export LD_LIBRARY_PATH=${ORACLE_HOME}/lib:${LD_LIBRARY_PATH}
echo 'LD_LIBRARY_PATH: ' ${LD_LIBRARY_PATH}
