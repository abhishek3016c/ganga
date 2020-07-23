Create user testsrc identified by testsrc
default tablespace USERS
temporary tablespace temp
quota unlimited on users;
grant create session, alter session to testsrc;
grant connect,resource to testsrc;
