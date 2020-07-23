set owner for a12
set object_name for a30

select owner,object_name,object_type from dba_objects where owner in
('TESTSRC','TESTTGT','GGSUSER')
/
