Create user ggsuser identified by ggsuser
default tablespace USERS
temporary tablespace temp
quota unlimited on users;

grant select any table to ggsuser;
grant select any dictionary to ggsuser;
grant flashback any table to ggsuser;
grant execute on dbms_flashback to ggsuser;
grant create session, alter session to ggsuser;
grant connect,resource to ggsuser;
grant alter any table to ggsuser;
