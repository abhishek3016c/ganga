#!/bin/sh
#
# $Header: rdbms/demo/xstream/fbr/java/xsdemo_env.sh /main/1 2009/02/27 15:57:39 tianli Exp $
#
# xsdemo_env.sh
#
# Copyright (c) 2009, Oracle and/or its affiliates.All rights reserved. 
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
#    tianli      02/20/09 - Creation
#
########################################

setenv CLASSPATH .:${ORACLE_HOME}/rdbms/jlib/xstreams.jar:${ORACLE_HOME}/dbjava/lib/ojdbc5.jar:${CLASSPATH}

echo 'CLASSPATH: ' ${CLASSPATH}

setenv PATH .:${ORACLE_HOME}/bin:${ORACLE_HOME}/jdk/bin:${PATH}
echo 'PATH: ' ${PATH}

setenv LD_LIBRARY_PATH ${ORACLE_HOME}/lib:${LD_LIBRARY_PATH}
echo 'LD_LIBRARY_PATH: ' ${LD_LIBRARY_PATH}
