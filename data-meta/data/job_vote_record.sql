create table vote_record(
ID BIGINT
,USER_ID STRING
,VOTE_ID BIGINT
,GROUP_ID BIGINT
,CREATE_TIME TIMESTAMP
)
row format delimited
stored as orc
location '/apps/hive/warehouse/vote_record';
