DROP TRIGGER IF EXISTS `sdltgd_AFTER_INSERT`;
DROP TRIGGER IF EXISTS `sdltgd_AFTER_UPDATE`;
DROP TRIGGER IF EXISTS `sdltgd_AFTER_DELETE`;

DROP PROCEDURE IF EXISTS `sdltgd_UPDATE_REDIS`;
DROP PROCEDURE IF EXISTS `sdltgd_REMOVE_REDIS`;
DROP PROCEDURE IF EXISTS `sdltgd_INIT_REDIS`;
DROP PROCEDURE IF EXISTS `sdltgd_INIT_BYDATE`;


DELIMITER $

CREATE TRIGGER `sdltgd_AFTER_INSERT` AFTER INSERT ON `sdltgd` FOR EACH ROW
BEGIN
  call sdltgd_UPDATE_REDIS( new.id,
														new.SCODE,
														new.RDATE,
														new.SHAREHDCODE,
														new.SHAREHDNUM,
														new.LTAG,
														new.ZB,
														new.NDATE,
														new.BZ,
														new.BDBL,
														new.SHAREHDNAME,
														new.SHAREHDTYPE,
														new.SHARESTYPE,
														new.RANK,
														new.SHAREHDRATIO,
														new.BDSUM,
														new.COMPANYCODE);
END $

CREATE TRIGGER `sdltgd_AFTER_UPDATE` AFTER UPDATE ON `sdltgd` FOR EACH ROW
BEGIN
   call sdltgd_REMOVE_REDIS(old.id,
														old.SCODE,
														old.RDATE,
														old.SHAREHDCODE,
														old.SHAREHDNUM,
														old.LTAG,
														old.ZB,
														old.NDATE,
														old.BZ,
														old.BDBL,
														old.SHAREHDNAME,
														old.SHAREHDTYPE,
														old.SHARESTYPE,
														old.RANK,
														old.SHAREHDRATIO,
														old.BDSUM,
														old.COMPANYCODE);
	
   call sdltgd_UPDATE_REDIS(new.id,
														new.SCODE,
														new.RDATE,
														new.SHAREHDCODE,
														new.SHAREHDNUM,
														new.LTAG,
														new.ZB,
														new.NDATE,
														new.BZ,
														new.BDBL,
														new.SHAREHDNAME,
														new.SHAREHDTYPE,
														new.SHARESTYPE,
														new.RANK,
														new.SHAREHDRATIO,
														new.BDSUM,
														new.COMPANYCODE);
END $


CREATE TRIGGER `sdltgd_AFTER_DELETE` AFTER DELETE ON `sdltgd` FOR EACH ROW
BEGIN
  call sdltgd_REMOVE_REDIS( old.id,
														old.SCODE,
														old.RDATE,
														old.SHAREHDCODE,
														old.SHAREHDNUM,
														old.LTAG,
														old.ZB,
														old.NDATE,
														old.BZ,
														old.BDBL,
														old.SHAREHDNAME,
														old.SHAREHDTYPE,
														old.SHARESTYPE,
														old.RANK,
														old.SHAREHDRATIO,
														old.BDSUM,
														old.COMPANYCODE);
END $


CREATE PROCEDURE sdltgd_UPDATE_REDIS
(
	in id int,
	in SCODE VARCHAR(10),
	in RDATE DATE,
	in SHAREHDCODE VARCHAR(45),
	in SHAREHDNUM DECIMAL(13,0),
	in LTAG DECIMAL(15,2),
	in ZB DECIMAL(13,2),
	in NDATE DATE,
	in BZ VARCHAR(10),
	in BDBL DECIMAL(13,2),
	in SHAREHDNAME VARCHAR(126),
	in SHAREHDTYPE VARCHAR(10),
	in SHARESTYPE VARCHAR(10),
	in RANK DECIMAL(4,0),
	in SHAREHDRATIO DECIMAL(13,2),
	in BDSUM DECIMAL(13,2),
	in COMPANYCODE VARCHAR(45)
) 
BEGIN
	SET @tableId = 'sdltgd';

    -- 更新缓存
	SELECT CONCAT_WS('_', @tableId, CAST(id AS CHAR)) into @NID;

    -- 字段
	SELECT redis_hset(@NID, 'id', CAST(id AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'SCode', SCode) INTO @tmp;   
	SELECT redis_hset(@NID, 'RDate', DATE_FORMAT(RDate, '%Y-%m-%d') )  INTO @tmp;    
	SELECT redis_hset(@NID, 'SHAREHDCODE', SHAREHDCODE) INTO @tmp;   
	SELECT redis_hset(@NID, 'SHAREHDNUM', CAST(SHAREHDNUM AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'LTAG', CAST(LTAG AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'ZB', CAST(ZB AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'NDATE', DATE_FORMAT(NDATE, '%Y-%m-%d') )  INTO @tmp;    
	SELECT redis_hset(@NID, 'BZ', BZ) INTO @tmp;   
	SELECT redis_hset(@NID, 'BDBL', CAST(BDBL AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'SHAREHDNAME', SHAREHDNAME) INTO @tmp;   
	SELECT redis_hset(@NID, 'SHAREHDTYPE', SHAREHDTYPE) INTO @tmp;   
	SELECT redis_hset(@NID, 'SHARESTYPE', SHARESTYPE) INTO @tmp;   
	SELECT redis_hset(@NID, 'RANK', CAST(RANK AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'SHAREHDRATIO', CAST(SHAREHDRATIO AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'BDSUM', CAST(BDSUM AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'COMPANYCODE', COMPANYCODE) INTO @tmp;   


    -- 索引
	SELECT redis_set( CONCAT_WS('_', @tableId, SCode, DATE_FORMAT(RDate, '%Y-%m-%d'), SHAREHDCODE), CAST(id AS CHAR)) INTO @tmp ;
	SELECT redis_set( CONCAT_WS('_', @tableId, DATE_FORMAT(RDate, '%Y-%m-%d'), SCode, SHAREHDCODE), CAST(id AS CHAR)) INTO @tmp ;
	SELECT redis_set( CONCAT_WS('_', @tableId, SHAREHDCODE, DATE_FORMAT(RDate, '%Y-%m-%d'), SCode), CAST(id AS CHAR)) INTO @tmp ;

END $


CREATE PROCEDURE sdltgd_REMOVE_REDIS
(
	in id int,
	in SCODE VARCHAR(10),
	in RDATE DATE,
	in SHAREHDCODE VARCHAR(45),
	in SHAREHDNUM DECIMAL(13,0),
	in LTAG DECIMAL(15,2),
	in ZB DECIMAL(13,2),
	in NDATE DATE,
	in BZ VARCHAR(10),
	in BDBL DECIMAL(13,2),
	in SHAREHDNAME VARCHAR(126),
	in SHAREHDTYPE VARCHAR(10),
	in SHARESTYPE VARCHAR(10),
	in RANK DECIMAL(4,0),
	in SHAREHDRATIO DECIMAL(13,2),
	in BDSUM DECIMAL(13,2),
	in COMPANYCODE VARCHAR(45)
) 
BEGIN
  SET @tableId = 'sdltgd';
	
  SELECT redis_del(CONCAT_WS('_', @tableId, CAST(id AS CHAR))) INTO @tmp ;   
  SELECT redis_del(CONCAT_WS('_', @tableId, SCode, DATE_FORMAT(RDate, '%Y-%m-%d'), SHAREHDCODE)) INTO @tmp ;
  SELECT redis_del(CONCAT_WS('_', @tableId, DATE_FORMAT(RDate, '%Y-%m-%d'), SCode, SHAREHDCODE)) INTO @tmp ;
  SELECT redis_del(CONCAT_WS('_', @tableId, SHAREHDCODE, DATE_FORMAT(RDate, '%Y-%m-%d'), SCode)) INTO @tmp ;

END $

CREATE PROCEDURE `sdltgd_INIT_REDIS`(
)
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
  DECLARE done boolean default 0;
  DECLARE mRDate DATE;
	
	DECLARE records CURSOR
	FOR
		SELECT distinct t.RDate FROM sdltgd t where t.RDate is not null;
		
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND  SET done=1; 
	
    -- 打开游标
	OPEN records;
    
	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO mRDate;
		
	   IF done != 1 THEN
	     -- 调用另一个存储过程获取结果
		 	 CALL sdltgd_INIT_BYDATE(mRDate);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	-- 在Redis中，表示该表已经缓存；日期时间数据用T分隔，用空格无法缓存
	SELECT redis_hset('CACHED_TABLES', 'sdltgd',  DATE_FORMAT(NOW(), '%Y-%m-%d %T')) INTO @tmp;  		 
	
END $


CREATE PROCEDURE `sdltgd_INIT_BYDATE`(
	in mRDate Date
)
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
  DECLARE done boolean default 0;
   
	DECLARE id int;
	DECLARE SCODE VARCHAR(10);
	DECLARE RDATE DATE;
	DECLARE SHAREHDCODE VARCHAR(45);
	DECLARE SHAREHDNUM DECIMAL(13,0);
	DECLARE LTAG DECIMAL(15,2);
	DECLARE ZB DECIMAL(13,2);
	DECLARE NDATE DATE;
	DECLARE BZ VARCHAR(10);
	DECLARE BDBL DECIMAL(13,2);
	DECLARE SHAREHDNAME VARCHAR(126);
	DECLARE SHAREHDTYPE VARCHAR(10);
	DECLARE SHARESTYPE VARCHAR(10);
	DECLARE RANK DECIMAL(4,0);
	DECLARE SHAREHDRATIO DECIMAL(13,2);
	DECLARE BDSUM DECIMAL(13,2);
	DECLARE COMPANYCODE VARCHAR(45);

    
	-- 声明游标
	-- cursor定义时,如果取出列中包含主键id，必须为表定义别名，否则fetch出值为0；非主键列未发现此问题。特此记录以备忘~ 
	DECLARE records CURSOR
	FOR
		SELECT t.id,t.SCODE,t.RDATE,t.SHAREHDCODE,t.SHAREHDNUM,t.LTAG,t.ZB,t.NDATE,t.BZ,
					 t.BDBL,t.SHAREHDNAME,t.SHAREHDTYPE,t.SHARESTYPE,t.RANK,t.SHAREHDRATIO,
					 t.BDSUM,t.COMPANYCODE
    FROM sdltgd t where t.RDate=mRDate;
	
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND  SET done=1; 
	
    -- 打开游标
	OPEN records;
    

	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO id,SCODE,RDATE,SHAREHDCODE,SHAREHDNUM,LTAG,ZB,NDATE,BZ,BDBL,
		 			SHAREHDNAME,SHAREHDTYPE,SHARESTYPE,RANK,SHAREHDRATIO,BDSUM,COMPANYCODE;
		
	   IF done !=1 THEN
	    -- 调用另一个存储过程获取结果
			CALL sdltgd_UPDATE_REDIS(id,SCODE,RDATE,SHAREHDCODE,SHAREHDNUM,LTAG,ZB,NDATE,BZ,BDBL,
		 			SHAREHDNAME,SHAREHDTYPE,SHARESTYPE,RANK,SHAREHDRATIO,BDSUM,COMPANYCODE);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	
END $



DELIMITER ;