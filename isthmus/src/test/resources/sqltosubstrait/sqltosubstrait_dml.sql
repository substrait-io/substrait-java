delete from src1 where intcol=10;
update src1 set intcol=10 where charcol='a';
insert into src1(intcol, charcol) values (1,'a');
insert into src1 select * from src2;
