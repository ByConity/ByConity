
-- source include/have_ssl_crypto_functs.inc

--disable_warnings
drop table if exists t1;
--enable_warnings

create table t1 (x blob);
insert into t1 values (DES_ENCRYPT('The quick red fox jumped over the lazy brown dog','The quick red fox jumped over the lazy brown dog'));
insert into t1 values (DES_ENCRYPT('a','The quick red fox jumped over the lazy brown dog'));
insert into t1 values (DES_ENCRYPT('The quick red fox jumped over the lazy brown dog','a'));
insert into t1 values (DES_ENCRYPT('ab','The quick red fox jumped over the lazy brown dog'));
insert into t1 values (DES_ENCRYPT('The quick red fox jumped over the lazy brown dog','ab'));
insert into t1 values (DES_ENCRYPT('abc','The quick red fox jumped over the lazy brown dog'));
insert into t1 values (DES_ENCRYPT('The quick red fox jumped over the lazy brown dog','abc'));
insert into t1 values (DES_ENCRYPT('abcd','The quick red fox jumped over the lazy brown dog'));
insert into t1 values (DES_ENCRYPT('The quick red fox jumped over the lazy brown dog','abcd'));
insert into t1 values (DES_ENCRYPT('abcde','The quick red fox jumped over the lazy brown dog'));
insert into t1 values (DES_ENCRYPT('The quick red fox jumped over the lazy brown dog','abcde'));
insert into t1 values (DES_ENCRYPT('abcdef','The quick red fox jumped over the lazy brown dog'));
insert into t1 values (DES_ENCRYPT('The quick red fox jumped over the lazy brown dog','abcdef'));
insert into t1 values (DES_ENCRYPT('abcdefg','The quick red fox jumped over the lazy brown dog'));
insert into t1 values (DES_ENCRYPT('The quick red fox jumped over the lazy brown dog','abcdefg'));
insert into t1 values (DES_ENCRYPT('abcdefgh','The quick red fox jumped over the lazy brown dog'));
insert into t1 values (DES_ENCRYPT('The quick red fox jumped over the lazy brown dog','abcdefgh'));
insert into t1 values (DES_ENCRYPT('abcdefghi','The quick red fox jumped over the lazy brown dog'));
insert into t1 values (DES_ENCRYPT('The quick red fox jumped over the lazy brown dog','abcdefghi'));
insert into t1 values (DES_ENCRYPT('abcdefghij','The quick red fox jumped over the lazy brown dog'));
insert into t1 values (DES_ENCRYPT('The quick red fox jumped over the lazy brown dog','abcdefghij'));
insert into t1 values (DES_ENCRYPT('abcdefghijk','The quick red fox jumped over the lazy brown dog'));
insert into t1 values (DES_ENCRYPT('The quick red fox jumped over the lazy brown dog','abcdefghijk'));
insert into t1 values (DES_ENCRYPT('The quick red fox jumped over the lazy brown dog','sabakala'));
insert into t1 values (DES_ENCRYPT('quick red fox jumped over the lazy brown dog','sabakala'));
insert into t1 values (DES_ENCRYPT('red fox jumped over the lazy brown dog','sabakala'));
insert into t1 values (DES_ENCRYPT('fox jumped over the lazy brown dog','sabakala'));
insert into t1 values (DES_ENCRYPT('jumped over the lazy brown dog','sabakala'));
insert into t1 values (DES_ENCRYPT('over the lazy brown dog','sabakala'));
insert into t1 values (DES_ENCRYPT('the lazy brown dog','sabakala'));
insert into t1 values (DES_ENCRYPT('lazy brown dog','sabakala'));
insert into t1 values (DES_ENCRYPT('brown dog','sabakala'));
insert into t1 values (DES_ENCRYPT('dog','sabakala'));
insert into t1 values (DES_ENCRYPT('dog!','sabakala'));
insert into t1 values (DES_ENCRYPT('dog!!','sabakala'));
insert into t1 values (DES_ENCRYPT('dog!!!','sabakala'));
insert into t1 values (DES_ENCRYPT('dog!!!!','sabakala'));
insert into t1 values (DES_ENCRYPT('dog!!!!!','sabakala'));
insert into t1 values (DES_ENCRYPT('jumped over the lazy brown dog','sabakala'));
insert into t1 values (DES_ENCRYPT('jumped over the lazy brown dog','sabakala'));
select hex(x), hex(DES_DECRYPT(x,'sabakala')) from t1;
select DES_DECRYPT(x,'sabakala') as s from t1 having s like '%dog%';
drop table t1;

--
-- Test default keys
--
select hex(DES_ENCRYPT('hello')),DES_DECRYPT(DES_ENCRYPT('hello'));
select DES_DECRYPT(DES_ENCRYPT('hello',4));
select DES_DECRYPT(DES_ENCRYPT('hello','test'),'test');
select hex(DES_ENCRYPT('hello')),hex(DES_ENCRYPT('hello',5)),hex(DES_ENCRYPT('hello','default_password'));
select DES_DECRYPT(DES_ENCRYPT('hello'),'default_password');
select DES_DECRYPT(DES_ENCRYPT('hello',4),'password4');

-- Test use of invalid parameters
--disable_ps_protocol
select DES_ENCRYPT('hello',10);
--enable_ps_protocol
select DES_ENCRYPT(NULL);
select DES_ENCRYPT(NULL, 10);
select DES_ENCRYPT(NULL, NULL);
--disable_ps_protocol
select DES_ENCRYPT(10, NULL);
--enable_ps_protocol
--disable_ps_protocol
select DES_ENCRYPT('hello', NULL);
--enable_ps_protocol

select DES_DECRYPT('hello',10);
select DES_DECRYPT(NULL);
select DES_DECRYPT(NULL, 10);
select DES_DECRYPT(NULL, NULL);
select DES_DECRYPT(10, NULL);
select DES_DECRYPT('hello', NULL);


-- Test flush
-- SET @a=DES_DECRYPT(DES_ENCRYPT('hello'));
flush des_key_file;
select @a = DES_DECRYPT(DES_ENCRYPT('hello'));

-- Test usage of wrong password
select hex('hello');
select hex(DES_DECRYPT(DES_ENCRYPT('hello',4),'password2'));
--disable_ps_protocol
select hex(DES_DECRYPT(DES_ENCRYPT('hello','hidden')));
--enable_ps_protocol

explain extended select DES_DECRYPT(DES_ENCRYPT('hello',4),'password2'), DES_DECRYPT(DES_ENCRYPT('hello','hidden'));

-- End of 4.1 tests

--
-- Bug#44365 valgrind warnings with encrypt() function
--
--disable_warnings
drop table if exists t1;
--enable_warnings
create table t1 (f1 smallint(6) default null, f2  mediumtext character set utf8)
default charset=latin1;
insert into t1 values (null,'contraction\'s');
insert into t1 values (-15818,'requirement\'s');
--disable_result_log
select encrypt(f1,f2) as a from t1,(select encrypt(f1,f2) as b from t1) a;
--enable_result_log
drop table t1;
