create table emp(
EMPNO BIGINT
,ENAME STRING
,JOB STRING
,MGR BIGINT
,HIREDATE DATE
,SAL BIGINT
,COMM BIGINT
,DEPTNO BIGINT
)
row format delimited
stored as orc
location '/apps/hive/warehouse/emp';
