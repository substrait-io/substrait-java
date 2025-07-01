delete from src1 where intcol=10;
update src1 set intcol=10 where charcol='a';
insert into src1(intcol, charcol) values (1,'a');
insert into src1 select * from src2;
create view dst1 as select * from src1;
create table dst1 as select * from src1;